require('dotenv').config();
const sql = require('mssql');
const axios = require('axios');
const fastify = require('fastify')({ logger: true });

// --- CONFIGURAÇÕES ---
const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 }
};

const SITE_API_URL = process.env.SITE_URL;
const SECRET_TOKEN = process.env.SYNC_TOKEN;
let pool;

// --- CONEXÃO BANCO ---
async function conectarBanco() {
    try {
        pool = await sql.connect(dbConfig);
        console.log(`✅ Conectado ao banco ${dbConfig.database}`);
    } catch (err) {
        console.error("❌ Falha na conexão inicial:", err.message);
        setTimeout(conectarBanco, 5000);
    }
}

// --- TAREFA 1: MONITOR DE ESTOQUE (LOOP) ---
async function monitorarMudancas() {
    if (!pool || !pool.connected) return;
    try {
        const query = `
            SELECT 
                P.CBARRA as sku,
                MAX(P.NOME) as name,
                SUM(CASE WHEN L.EST_ATUAL < 0 THEN 0 ELSE L.EST_ATUAL END) as total_stock, 
                MAX(L.PCO_VENDA) as price
            FROM PRODUTOS P
            INNER JOIN PRODLOJAS L ON P.CODIGO = L.CODIGO
            WHERE 
                P.DATA_ALTERACAO >= DATEADD(second, -5, GETDATE())
                AND P.CBARRA IS NOT NULL AND P.CBARRA <> ''
            GROUP BY P.CBARRA
        `;

        const result = await pool.request().query(query);

        if (result.recordset.length > 0) {
            console.log(`📡 Enviando atualização de ${result.recordset.length} produtos...`);
            await axios.post(SITE_API_URL, { updates: result.recordset }, {
                headers: { 'Authorization': `Bearer ${SECRET_TOKEN}` },
                timeout: 3000
            });
        }
    } catch (err) {
        if (err.code !== 'ECONNREFUSED' && err.code !== 'ETIMEDOUT') {
            console.error("Erro no ciclo de monitoramento:", err.message);
        }
    }
}

// --- TAREFA 2: API PARA RECEBER PEDIDOS E CHECAR ESTOQUE ---
fastify.get('/checar-estoque/:sku', async (request, reply) => {
    if (!pool || !pool.connected) return reply.code(503).send({ error: "Banco Offline" });
    const { sku } = request.params;
    try {
        const result = await pool.request().input('sku', sql.VarChar, sku).query(`
            SELECT SUM(CASE WHEN EST_ATUAL < 0 THEN 0 ELSE EST_ATUAL END) as estoque 
            FROM PRODUTOS P INNER JOIN PRODLOJAS L ON P.CODIGO = L.CODIGO WHERE P.CBARRA = @sku
        `);
        return { sku, estoque: result.recordset[0]?.estoque || 0 };
    } catch (err) { return reply.code(500).send({ error: err.message }); }
});

fastify.post('/criar-pedido', async (request, reply) => {
    if (!pool || !pool.connected) return reply.code(503).send({ error: "Banco Offline" });

    const { cliente, itens } = request.body; 
    // Espera receber: { cliente: { nome: "João" }, itens: [{ sku: "123", qtd: 1, preco: 100 }] }

    const transaction = new sql.Transaction(pool);
    
    try {
        await transaction.begin(); 

        for (let item of itens) {
            const prod = await transaction.request()
                .input('sku', sql.VarChar, item.sku)
                .query("SELECT TOP 1 CODIGO, NOME FROM PRODUTOS WHERE CBARRA = @sku");
            
            if (prod.recordset.length === 0) throw new Error(`Produto não encontrado: ${item.sku}`);
            item.codigoInterno = prod.recordset[0].CODIGO;
            item.descricao = prod.recordset[0].NOME;
        }

        const ids = await transaction.request().query(`
            SELECT 
                ISNULL(MAX(CODIGO), 0) + 1 as proximoId,
                ISNULL(MAX(SEQUENCIA), 0) + 1 as proximaSeq
            FROM ORCPERSON
        `);
        const novoId = ids.recordset[0].proximoId;
        const novaSeq = ids.recordset[0].proximaSeq;

        await transaction.request()
            .input('id', sql.Int, novoId)
            .input('seq', sql.Int, novaSeq)
            .input('nome', sql.VarChar, cliente.nome || 'CLIENTE E-COMMERCE')
            .input('total', sql.Money, itens.reduce((acc, i) => acc + (i.qtd * i.preco), 0))
            .query(`
                INSERT INTO ORCPERSON (CODIGO, CODLOJA, EMISSAO, NOME, CODVENDEDOR, TOTALBRUTO, TOTALLIQUIDO, OBSERVACAO, NOMEUSUARIO, SEQUENCIA)
                VALUES (@id, 1, GETDATE(), @nome, 1, @total, @total, 'PEDIDO VIA SITE', 'SITE', @seq)
            `);

        let contadorItem = 1;
        for (let item of itens) {
            await transaction.request()
                .input('idPedido', sql.Int, novoId)
                .input('idProd', sql.Int, item.codigoInterno)
                .input('desc', sql.VarChar, item.descricao)
                .input('qtd', sql.Float, item.qtd)
                .input('preco', sql.Money, item.preco)
                .input('total', sql.Money, item.qtd * item.preco)
                .input('itemNum', sql.Int, contadorItem++)
                .query(`
                    INSERT INTO ORCPERSONI (CODIGOI, CODPRODUTO, DESCRICAO, QUANTIDADE, UNITARIO, VLTOTAL, DATA, ITEM)
                    VALUES (@idPedido, @idProd, @desc, @qtd, @preco, @total, GETDATE(), @itemNum)
                `);
        }

        await transaction.commit(); 
        console.log(`✅ Pedido ${novoId} criado com sucesso!`);
        return { success: true, pedidoId: novoId };

    } catch (err) {
        if (transaction.active) await transaction.rollback(); 
        console.error("Erro ao criar pedido:", err.message);
        return reply.code(500).send({ error: "Erro ao gravar pedido", detalhe: err.message });
    }
});

// --- INICIALIZAÇÃO ---
const start = async () => {
    await conectarBanco();
    try {
        await fastify.listen({ port: 3005, host: '0.0.0.0' });
        console.log('🚀 Agente Integração rodando na porta 3005');
        setInterval(monitorarMudancas, 2000);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};

start();
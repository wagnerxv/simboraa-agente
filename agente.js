require('dotenv').config();
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const axios = require('axios');
const fastify = require('fastify')({ 
    logger: {
        level: 'info',
        transport: { target: 'pino-pretty' } 
    }
});

// --- CONFIGURAÇÕES E CONSTANTES ---
const DB_CONFIG = {
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    options: {
        encrypt: false, 
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: { 
        max: 10, 
        min: 0, 
        idleTimeoutMillis: 30000 
    }
};

const SITE_API_URL = process.env.SITE_URL; 
const SYNC_TOKEN = process.env.SYNC_TOKEN; 
const AGENT_TOKEN = process.env.AGENT_TOKEN; 
const CURSOR_FILE = path.join(__dirname, 'cursor.json');

let pool;

// --- GERENCIAMENTO DE ESTADO (CURSOR) ---
function getLastSyncTime() {
    try {
        if (fs.existsSync(CURSOR_FILE)) {
            const data = JSON.parse(fs.readFileSync(CURSOR_FILE, 'utf8'));
            return new Date(data.lastSync);
        }
    } catch (err) {
        console.error("⚠️ Erro ao ler cursor, usando padrão de 1 min atrás.");
    }
    return new Date(Date.now() - 60000); 
}

function updateLastSyncTime(date) {
    try {
        fs.writeFileSync(CURSOR_FILE, JSON.stringify({ lastSync: date.toISOString() }));
    } catch (err) {
        console.error("⚠️ Erro ao salvar cursor:", err.message);
    }
}

// --- CONEXÃO BANCO COM RETRY ---
async function conectarBanco() {
    try {
        pool = await sql.connect(DB_CONFIG);
        console.log(`✅ [SQL] Conectado ao banco ${DB_CONFIG.database}`);
        
        pool.on('error', err => {
            console.error('❌ [SQL] Erro na pool de conexão:', err);
        });

    } catch (err) {
        console.error("❌ [SQL] Falha na conexão inicial:", err.message);
        console.log("🔄 Tentando novamente em 5 segundos...");
        setTimeout(conectarBanco, 5000);
    }
}

// --- SEGURANÇA ---
fastify.addHook('onRequest', async (request, reply) => {
    if (request.routerPath === '/health') return;

    const authHeader = request.headers['authorization'];
    if (!authHeader || authHeader !== `Bearer ${AGENT_TOKEN}`) {
        reply.code(401).send({ error: "Acesso Negado", message: "Token inválido ou ausente" });
    }
});

// --- TAREFA 1: MONITOR DE ESTOQUE (LOOP) ---
async function monitorarMudancas() {
    if (!pool || !pool.connected) return;

    const lastCheck = getLastSyncTime();
    const newCheckTime = new Date(); 

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
                P.DATA_ALTERACAO > @lastCheck
                AND P.CBARRA IS NOT NULL AND P.CBARRA <> ''
            GROUP BY P.CBARRA
        `;

        const result = await pool.request()
            .input('lastCheck', sql.DateTime, lastCheck)
            .query(query);

        if (result.recordset.length > 0) {
            console.log(`📡 [SYNC] Detectadas alterações em ${result.recordset.length} produtos.`);
            
            await axios.post(SITE_API_URL, { updates: result.recordset }, {
                headers: { 
                    'Authorization': `Bearer ${SYNC_TOKEN}`,
                    'Content-Type': 'application/json'
                },
                timeout: 10000 
            });
            
            console.log(`✅ [SYNC] Enviado com sucesso.`);
            updateLastSyncTime(newCheckTime);
        } else {
            updateLastSyncTime(newCheckTime);
        }

    } catch (err) {
        console.error("⚠️ [SYNC] Erro no ciclo de monitoramento:", err.message);
    }
}

// --- ENDPOINTS DA API ---
fastify.get('/health', async () => {
    return { status: 'ok', db: pool && pool.connected ? 'connected' : 'disconnected' };
});

fastify.get('/checar-estoque/:sku', async (request, reply) => {
    if (!pool || !pool.connected) return reply.code(503).send({ error: "Banco Offline" });
    
    const { sku } = request.params;
    try {
        const result = await pool.request()
            .input('sku', sql.VarChar, sku)
            .query(`
                SELECT SUM(CASE WHEN EST_ATUAL < 0 THEN 0 ELSE EST_ATUAL END) as estoque 
                FROM PRODUTOS P 
                INNER JOIN PRODLOJAS L ON P.CODIGO = L.CODIGO 
                WHERE P.CBARRA = @sku
            `);
        
        return { sku, estoque: result.recordset[0]?.estoque || 0 };
    } catch (err) {
        request.log.error(err);
        return reply.code(500).send({ error: "Erro interno ao consultar estoque" });
    }
});

fastify.post('/criar-pedido', async (request, reply) => {
    if (!pool || !pool.connected) return reply.code(503).send({ error: "Banco Offline" });

    const { cliente, itens } = request.body; 
    
    if (!cliente || !itens || itens.length === 0) {
        return reply.code(400).send({ error: "Dados inválidos (cliente ou itens ausentes)" });
    }

    const transaction = new sql.Transaction(pool);
    
    try {
        await transaction.begin(); 

        for (let item of itens) {
            const prod = await transaction.request()
                .input('sku', sql.VarChar, item.sku)
                .query("SELECT TOP 1 CODIGO, NOME, PCO_VENDA FROM PRODUTOS WHERE CBARRA = @sku");
            
            if (prod.recordset.length === 0) {
                throw new Error(`Produto não encontrado ou inativo: ${item.sku}`);
            }
            
            item.codigoInterno = prod.recordset[0].CODIGO;
            item.descricao = prod.recordset[0].NOME;
        }

        const idsQuery = `
            SELECT 
                ISNULL(MAX(CODIGO), 0) + 1 as proximoId,
                ISNULL(MAX(SEQUENCIA), 0) + 1 as proximaSeq
            FROM ORCPERSON WITH (UPDLOCK, HOLDLOCK)
        `;
        
        const ids = await transaction.request().query(idsQuery);
        const novoId = ids.recordset[0].proximoId;
        const novaSeq = ids.recordset[0].proximaSeq;

        const totalPedido = itens.reduce((acc, i) => acc + (i.qtd * i.preco), 0);
        
        await transaction.request()
            .input('id', sql.Int, novoId)
            .input('seq', sql.Int, novaSeq)
            .input('nome', sql.VarChar, cliente.nome ? cliente.nome.substring(0, 50) : 'CLIENTE WEB')
            .input('total', sql.Money, totalPedido)
            .query(`
                INSERT INTO ORCPERSON (CODIGO, CODLOJA, EMISSAO, NOME, CODVENDEDOR, TOTALBRUTO, TOTALLIQUIDO, OBSERVACAO, NOMEUSUARIO, SEQUENCIA)
                VALUES (@id, 1, GETDATE(), @nome, 1, @total, @total, 'PEDIDO VIA SITE', 'INTEGRACAO', @seq)
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
        console.log(`✅ [PEDIDO] Pedido ${novoId} criado com sucesso!`);
        return { success: true, pedidoId: novoId };

    } catch (err) {
        if (transaction.active) await transaction.rollback(); 
        console.error("❌ [PEDIDO] Erro ao processar:", err.message);
        
        return reply.code(422).send({ 
            error: "Não foi possível processar o pedido", 
            detalhe: err.message 
        });
    }
});

// --- INICIALIZAÇÃO ---
const start = async () => {
    await conectarBanco();
    try {
        await fastify.listen({ port: 3005, host: '0.0.0.0' });
        console.log('🚀 Agente Integração rodando na porta 3005');
        
        setInterval(monitorarMudancas, 5000);
        
    } catch (err) {
        fastify.log.error(err);
        process.exit(1);
    }
};

start();
// workers/alertProcessor.js
// FINAL CORRECTED VERSION

const { Worker } = require('bullmq');
const { createClient } = require('@supabase/supabase-js');
const { Pool } = require('pg');
const { Redis } = require('ioredis');
// const { sendMessageWithBotToken } = require('./slackWebApiNotifier');
const { sendNotificationToSlack } = require('./sendSlackNotification');

// --- Client Initializations ---
const redisConnection = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
const QUEUE_NAME = 'alert-queue';

// --- Helper Functions (Mirrors the logic from your /api/executequery route) ---

// This function now returns a complete PoolConfig object, not just a string.
function getDatabaseConfig(connectionString) {
    const config = JSON.parse(connectionString);
    const poolConfig = {
        host: config.host,
        database: config.database,
        user: config.username || config.user, // Handle both username/user keys
        port: parseInt(config.port, 10),
        password: config.password,
    };

    // Explicitly handle SSL based on the stored config, defaulting to secure.
    if (config.ssl === false) {
        poolConfig.ssl = false;
    } else {
        // For Neon, Supabase, and other cloud DBs, this is the required setting.
        poolConfig.ssl = { rejectUnauthorized: false };
    }
    
    return poolConfig;
}

// This function now uses the full config object to create a Pool.
async function getPollData(poolConfig, sql) {
    const pool = new Pool(poolConfig);
    const client = await pool.connect();
    try {
        const result = await client.query(sql);
        if (!result.rows || result.rows.length === 0) throw new Error("SQL query returned no rows.");
        const value = parseFloat(Object.values(result.rows[0])[0]);
        if (isNaN(value)) throw new Error("SQL query did not return a numeric value.");
        return { value };
    } finally {
        client.release();
        await pool.end();
    }
}

function evaluateCondition(value, condition, threshold) {
    switch (condition) {
        case '>': return value > threshold;
        case '<': return value < threshold;
        case '>=': return value >= threshold;
        case '<=': return value <= threshold;
        case '=': return value === threshold;
        case '!=': return value !== threshold;
        default: throw new Error(`Unsupported condition: ${condition}`);
    }
}

// --- Main Worker Logic ---
const alertJobProcessor = async (job) => {
    // Create a NEW Supabase client for every job to guarantee a valid auth state.
    const supabase = createClient(
        process.env.NEXT_PUBLIC_SUPABASE_URL,
        process.env.SUPABASE_SERVICE_ROLE_KEY,
        { auth: { persistSession: false } }
    );

    const { ruleId, ruleType } = job.data;
    console.log(`[Worker] Processing job ${job.id} for ${ruleType} rule: ${ruleId}`);
    let logStatus = 'failed', logDetails = {}, ruleOwnerId = null;

    try {
        if (ruleType === 'metric' || ruleType === 'custom_kpi') {
            let rule, metric, kpi; // Declare variables

            if (ruleType === 'metric') {
                ({ data: rule, error: fetchError } = await supabase.from('autonomis_user_metric_rule_table').select('*').eq('id', ruleId).single());
                if (fetchError || !rule) throw new Error(`Metric Rule ${ruleId} not found.`);
                ruleOwnerId = rule.user_id;

                ({ data: metric, error: dbConfigError } = await supabase.from('autonomis_user_metrics_table').select('name, sql, autonomis_user_notebook(*, autonomis_user_database(*))').eq('id', rule.metric_table_id).single());
                if (dbConfigError || !metric.autonomis_user_notebook?.autonomis_user_database) throw new Error(`Data config for rule ${ruleId} is incomplete.`);
            } else { // custom_kpi
                ({ data: kpi, error: kpiError } = await supabase.from('autonomis_custom_kpi_alerts').select('*').eq('id', ruleId).single());
                if (kpiError || !kpi) throw new Error(`Custom KPI Rule ${ruleId} not found.`);
                ruleOwnerId = kpi.user_id;
            }

            const currentRule = rule || kpi;
            const isActive = ruleType === 'metric' ? currentRule.isActive : currentRule.is_active;

            if (!isActive) {
                logStatus = 'success';
                logDetails = { message: 'Rule is inactive, skipped.' };
            } else {
                let connectionStringJSON, sqlQuery, ruleName, operator, threshold;
                
                if (ruleType === 'metric') {
                    connectionStringJSON = JSON.stringify(metric.autonomis_user_notebook.autonomis_user_database.connection_string);
                    sqlQuery = metric.sql;
                    ruleName = metric.name || `Metric Rule ${rule.id}`;
                    operator = rule.operator;
                    threshold = rule.value;
                } else { // custom_kpi
                    const { data: db, error: dbError } = await supabase.from('autonomis_user_database').select('connection_string').eq('id', kpi.database_id).single();
                    if (dbError || !db) throw new Error(`Database connection for KPI ${ruleId} not found.`);
                    connectionStringJSON = db.connection_string;
                    sqlQuery = kpi.sql;
                    ruleName = kpi.name;
                    operator = kpi.condition_operator;
                    threshold = kpi.threshold_value;
                }

                // *** THIS IS THE NEW, CORRECTED FLOW ***
                // 1. Get the full config object.
                const dbPoolConfig = getDatabaseConfig(connectionStringJSON);
                // 2. Pass the config object to get the data.
                const newData = await getPollData(dbPoolConfig, sqlQuery);
                // *** END OF NEW FLOW ***
                
                const triggered = evaluateCondition(newData.value, operator, threshold);
                logStatus = triggered ? 'triggered' : 'success';
                logDetails = { value: newData.value, threshold: threshold, operator: operator };

                if (triggered) {
                    await sendNotificationToSlack({
                        name: ruleName, id: currentRule.id, currentValue: newData.value,
                        operator: operator, threshold: threshold,
                    });
                }
            }
        } else {
            throw new Error(`Unknown ruleType received: ${ruleType}`);
        }
    } catch (error) {
        logStatus = 'failed';
        logDetails = { error: error.message };
        console.error(`[Worker] ERROR processing job ${job.id}:`, error.message);
    }
    
    if (ruleOwnerId) {
        const { error: insertError } = await supabase.from('alert_history_logs').insert({
            rule_id: ruleId, status: logStatus, details: logDetails, user_id: ruleOwnerId
        });
        if (insertError) {
            console.error(`[Worker] FATAL: Failed to insert log into database!`, insertError);
        }
    }

    if (logStatus === 'failed') throw new Error(logDetails.error);
};

// --- Start the Worker ---
new Worker(QUEUE_NAME, alertJobProcessor, { connection: redisConnection });
console.log('[Worker] Worker started in JS mode. Listening for jobs from remote Redis...');
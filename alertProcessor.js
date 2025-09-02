// workers/alertProcessor.js
// FINAL CORRECTED VERSION

// --- Environment Variable Loading ---
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env.local') });
// --- End of Loading ---

const { Worker } = require('bullmq');
const { createClient } = require('@supabase/supabase-js');
const { Pool } = require('pg');
const { Redis } = require('ioredis');
const { sendMessageWithBotToken } = require('./slackWebApiNotifier');

// --- Client Initializations ---
const redisConnection = new Redis(process.env.ALERT_URL, { maxRetriesPerRequest: null });
const QUEUE_NAME = 'alert-queue';

// --- Helper Functions (No changes needed here) ---
function getDatabaseConfig(connectionData) {
    const config = typeof connectionData === 'string' ? JSON.parse(connectionData) : connectionData;
    const poolConfig = {
        host: config.host,
        database: config.database,
        user: config.username || config.user,
        port: parseInt(config.port, 10),
        password: config.password,
        ssl: { rejectUnauthorized: false }
    };
    return poolConfig;
}

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
    const jobType = job.name;
    const jobData = job.data;
    const { jobId: ruleId, jobType: ruleType } = jobData;

    console.log(`[Worker] Processing '${jobType}' job ${job.id} for ID: ${ruleId}`);
    
    // --- REPORT TRIGGER LOGIC ---
    if (jobType === 'report') {
        try {
            // *** THIS IS THE CRITICAL FIX: "FIRE-AND-FORGET" ***
            // We are REMOVING the 'await' and the 'const response ='.
            // The worker will send the request and immediately continue, not waiting for a reply.
            fetch(`${process.env.NEXT_PUBLIC_BASE_URL}/api/report-trigger`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Worker-Secret': process.env.WORKER_SECRET_KEY
                },
                body: JSON.stringify({
                    slug: jobData.slug,
                    slackChannelId: jobData.slackChannelId,
                }),
            });
            // *** END OF FIX ***

            // We now log success immediately. The actual report generation happens in the background.
            console.log(`[Worker] Successfully TRIGGERED report for ID: ${ruleId}. The report is now generating in the background.`);

        } catch (error) {
            // This will only catch immediate errors, like if the Next.js server is down.
            console.error(`[Worker] ERROR sending trigger request for report ID ${ruleId}:`, error.message);
            throw error; // Still mark the job as failed if the trigger fails
        }
    } 
    // --- ALERT PROCESSING LOGIC (Unchanged and intact) ---
    else if (jobType === 'metric' || jobType === 'custom_kpi') {
        const supabase = createClient(
            process.env.NEXT_PUBLIC_SUPABASE_URL,
            process.env.SUPABASE_SERVICE_ROLE_KEY,
            { auth: { persistSession: false } }
        );
        let logStatus = 'failed', logDetails = {}, ruleOwnerId = null;

        try {
            let connectionData, sqlQuery, ruleName, operator, threshold, currentRule, isActive;

            if (ruleType === 'metric') {
                const { data: rule, error: fetchError } = await supabase.from('autonomis_user_metric_rule_table').select('*').eq('id', ruleId).single();
                if (fetchError || !rule) throw new Error(`Metric Rule ${ruleId} not found.`);
                ruleOwnerId = rule.user_id; currentRule = rule; isActive = rule.isActive;

                if (isActive) {
                    const { data: metric, error: dbConfigError } = await supabase.from('autonomis_user_metrics_table').select('name, sql, autonomis_user_notebook(*, autonomis_user_database(*))').eq('id', rule.metric_table_id).single();
                    if (dbConfigError || !metric?.autonomis_user_notebook?.autonomis_user_database) throw new Error(`Data config for Metric Rule ${ruleId} is incomplete.`);
                    connectionData = metric.autonomis_user_notebook.autonomis_user_database.connection_string;
                    sqlQuery = metric.sql; ruleName = metric.name || `Metric Rule ${rule.id}`;
                    operator = rule.operator; threshold = rule.value;
                }
            } else { // custom_kpi
                const { data: kpi, error: kpiError } = await supabase.from('autonomis_custom_kpi_alerts').select('*').eq('id', ruleId).single();
                if (kpiError || !kpi) throw new Error(`Custom KPI Rule ${ruleId} not found.`);
                ruleOwnerId = kpi.user_id; currentRule = kpi; isActive = kpi.is_active;

                if (isActive) {
                    const { data: db, error: dbError } = await supabase.from('autonomis_user_database').select('connection_string').eq('id', kpi.database_id).single();
                    if (dbError || !db) throw new Error(`Database connection for KPI ${ruleId} not found.`);
                    connectionData = db.connection_string;
                    sqlQuery = kpi.sql; ruleName = kpi.name;
                    operator = kpi.condition_operator; threshold = kpi.threshold_value;
                }
            }

            if (!isActive) {
                logStatus = 'success';
                logDetails = { message: 'Rule is inactive, skipped.' };
            } else {
                const dbPoolConfig = getDatabaseConfig(connectionData);
                const newData = await getPollData(dbPoolConfig, sqlQuery);
                const triggered = evaluateCondition(newData.value, operator, threshold);
                logStatus = triggered ? 'triggered' : 'success';
                logDetails = { value: newData.value, threshold: threshold, operator: operator };

                if (triggered) {
                    await sendMessageWithBotToken({
                        name: ruleName, id: currentRule.id, currentValue: newData.value,
                        operator: operator, threshold: threshold,
                    });
                } else {
                    console.log(`[Worker] Rule ${ruleId} completed successfully but was not triggered.`);
                }
            }
        } catch (error) {
            logStatus = 'failed';
            const errorMessage = error instanceof Error ? error.message : String(error);
            logDetails = { error: errorMessage };
            console.error(`[Worker] ERROR processing alert job ${job.id}:`, errorMessage);
        }

        if (ruleOwnerId) {
            await supabase.from('alert_history_logs').insert({
                rule_id: ruleId, status: logStatus, details: logDetails, user_id: ruleOwnerId
            });
        }

        if (logStatus === 'failed') throw new Error(logDetails.error);
    } 
    else {
        console.warn(`[Worker] Unknown job name received: ${jobType}`);
    }
};

// --- Start the Worker ---
new Worker(QUEUE_NAME, alertJobProcessor, { connection: redisConnection });
console.log('[Worker] Worker started in JS mode. Listening for all job types...');
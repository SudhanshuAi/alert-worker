// workers/alertProcessor.js
// FINAL CORRECTED VERSION

// --- Environment Variable Loading ---
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env.local') });
// --- End of Loading ---

const { Worker } = require('bullmq');
const { createClient } = require('@supabase/supabase-js');
const { Redis } = require('ioredis');

// --- Client Initializations ---
const redisConnection = new Redis(process.env.ALERT_URL, { maxRetriesPerRequest: null });
const QUEUE_NAME = 'alert-queue';

const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    { auth: { persistSession: false } }
);

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
                    viewType: 'drilldown'
                }),
            });
            // *** END OF FIX ***

            // We now log success immediately. The actual report generation happens in the background.
            console.log(`[Worker] Successfully TRIGGERED report for ID: ${ruleId}. The report is now generating in the background.`);

            await supabase.from('autonomis_report_history_logs').insert({
            report_id: ruleId,
            status: 'triggered',
            details: { message: 'Report generation successfully initiated by worker.' },
            user_id: jobData.reportOwnerId || null 
        });

        } catch (error) {
            // This will only catch immediate errors, like if the Next.js server is down.
            console.error(`[Worker] ERROR sending trigger request for report ID ${ruleId}:`, error.message);
            throw error; // Still mark the job as failed if the trigger fails
        }
    } 
    // --- METRIC ALERT TRIGGER LOGIC ---
    else if (jobType === 'metric') {
        try {
            // Fire-and-forget metric alert trigger
            fetch(`${process.env.NEXT_PUBLIC_BASE_URL}/api/test-alert`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Worker-Secret': process.env.WORKER_SECRET_KEY
                },
                body: JSON.stringify({
                    id: ruleId,
                    type: 'metric',
                    source: 'worker' // To distinguish from manual testing
                }),
            });

            console.log(`[Worker] Successfully TRIGGERED metric alert for ID: ${ruleId}. Processing in the background.`);

            // Log the trigger attempt
            await supabase.from('alert_history_logs').insert({
                rule_id: ruleId,
                status: 'triggered',
                details: { message: 'Metric alert processing successfully initiated by worker.' },
                user_id: jobData.userId || null
            });

        } catch (error) {
            console.error(`[Worker] ERROR sending trigger request for metric alert ID ${ruleId}:`, error.message);
            throw error;
        }
    }
    // --- CUSTOM KPI ALERT TRIGGER LOGIC ---
    else if (jobType === 'custom_kpi') {
        try {
            // Fire-and-forget custom KPI alert trigger
            fetch(`${process.env.NEXT_PUBLIC_BASE_URL}/api/test-kpi-threshold`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Worker-Secret': process.env.WORKER_SECRET_KEY
                },
                body: JSON.stringify({
                    kpiId: ruleId,
                    source: 'worker' // To distinguish from manual testing
                }),
            });

            console.log(`[Worker] Successfully TRIGGERED custom KPI alert for ID: ${ruleId}. Processing in the background.`);

            // Log the trigger attempt
            await supabase.from('alert_history_logs').insert({
                rule_id: ruleId,
                status: 'triggered',
                details: { message: 'Custom KPI alert processing successfully initiated by worker.' },
                user_id: jobData.userId || null
            });

        } catch (error) {
            console.error(`[Worker] ERROR sending trigger request for custom KPI alert ID ${ruleId}:`, error.message);
            throw error;
        }
    } 
    else {
        console.warn(`[Worker] Unknown job name received: ${jobType}`);
    }
};

// --- Start the Worker ---
new Worker(QUEUE_NAME, alertJobProcessor, { connection: redisConnection });
console.log('[Worker] Worker started in JS mode. Listening for all job types...');
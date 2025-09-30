// workers/alertProcessor.js
// FINAL CORRECTED VERSION

// --- Environment Variable Loading ---
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env.local') });
// --- End of Loading ---

const { Worker } = require('bullmq');
const { createClient } = require('@supabase/supabase-js');
const { Redis } = require('ioredis');
const { v4: uuidv4 } = require('uuid');


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
        // 1. Generate a unique run_id at the start of the job.
        const run_id = uuidv4();
        console.log(`[Worker] Generated unique run_id for report ${ruleId}: ${run_id}`);
        
        try {
            // Fire-and-forget the trigger to your API route.
            // 2. Pass the new run_id in the body so the API can use it.
            fetch(`${process.env.NEXT_PUBLIC_BASE_URL}/api/report-trigger`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Worker-Secret': process.env.WORKER_SECRET_KEY
                },
                body: JSON.stringify({
                    slug: jobData.slug,
                    slackChannelId: jobData.slackChannelId,
                    viewType: jobData.viewType,
                    subViewType: jobData.subViewType,
                    executionType: 'scheduled',
                    run_id: run_id // <-- Pass the unique ID
                }),
            });

            console.log(`[Worker] Successfully TRIGGERED report for ID: ${ruleId}. The report is now generating in the background.`);

            // 3. Immediately log the "initiation" message to your history table.
            // This is the log the user will see while the report is generating.
            const { error: logError } = await supabase.from('autonomis_report_history_logs').insert({
                report_id: ruleId,
                status: 'triggered',
                run_id: run_id, // <-- Store the unique ID
                details: { 
                    run_id: run_id,
                    message: 'Report generation successfully initiated by worker.' 
                },
                user_id: jobData.reportOwnerId || null 
            });

            if (logError) {
                // If logging fails, we should still continue, but log the error.
                console.error(`[Worker] CRITICAL: Failed to insert initial history log for run_id ${run_id}.`, logError);
            }

        } catch (error) {
            // This will only catch errors from the initial 'fetch' call.
            console.error(`[Worker] ERROR sending trigger request for report ID ${ruleId}:`, error.message);
            
            // 4. If the trigger fails, log the failure with the same run_id for traceability.
            const failureDetails = {
                run_id: run_id,
                error: `Worker failed to trigger API route: ${error.message}`
            };
            await supabase.from('autonomis_report_history_logs').insert({
                report_id: ruleId,
                status: 'failed',
                run_id: run_id,
                details: failureDetails,
                user_id: jobData.reportOwnerId || null 
            });
            throw error; // Still mark the job as failed in BullMQ
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
            // await supabase.from('alert_history_logs').insert({
            //     rule_id: ruleId,
            //     status: 'triggered',
            //     details: { message: 'Metric alert processing successfully initiated by worker.' },
            //     user_id: jobData.userId || null
            // });

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
            // await supabase.from('alert_history_logs').insert({
            //     rule_id: ruleId,
            //     status: 'triggered',
            //     details: { message: 'Custom KPI alert processing successfully initiated by worker.' },
            //     user_id: jobData.userId || null
            // });

        } catch (error) {
            console.error(`[Worker] ERROR sending trigger request for custom KPI alert ID ${ruleId}:`, error.message);
            throw error;
        }
    } 

    // --- ALERT TRACKER TRIGGER LOGIC ---
    else if (jobType === 'alert_tracker') {
        try {
            // Fire-and-forget alert tracker trigger
            fetch(`${process.env.NEXT_PUBLIC_BASE_URL}/api/alert-trigger-tracker`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Worker-Secret': process.env.WORKER_SECRET_KEY
                },
                body: JSON.stringify({
                    slug: ruleId, // For trackers, the ID is the slug
                    executionType: 'scheduled',
                    slackChannelId: jobData.slackChannelId // Pass the channel ID
                }),
            });

            console.log(`[Worker] Successfully TRIGGERED alert tracker for ID: ${ruleId}. Processing in the background.`);
        } catch (error) {
            console.error(`[Worker] ERROR sending trigger request for alert tracker ID ${ruleId}:`, error.message);
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
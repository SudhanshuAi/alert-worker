// workers/slackWebApiNotifier.js
const { WebClient } = require('@slack/web-api');

// --- Load Credentials from .env.local ---
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;

// This is the hardcoded Channel ID from your /api/slack/route.ts file.
// All scheduled alerts will be sent to this channel.
const CHANNEL_ID = 'C08CTTHQRDY'; 
// --- End Credentials ---

let webClient; // Initialize as undefined

if (SLACK_BOT_TOKEN) {
    webClient = new WebClient(SLACK_BOT_TOKEN);
}

async function sendMessageWithBotToken(details) {
    if (!webClient) {
        console.error("[Worker] SLACK_BOT_TOKEN is not defined in .env.local. Cannot send notification.");
        return;
    }

    if (!CHANNEL_ID) {
        console.error("[Worker] Slack Channel ID is not defined. Cannot send notification.");
        return;
    }

    const messageBlocks = [
        {
            type: 'section',
            text: {
                type: 'mrkdwn',
                text: `*🚨 Alert Triggered: ${details.name}*`,
            },
        },
        {
            type: 'section',
            fields: [
                { type: 'mrkdwn', text: `*Condition Met:*\n\`${details.currentValue} ${details.operator} ${details.threshold}\`` },
                { type: 'mrkdwn', text: `*Status:*\n🔥 Triggered` },
            ],
        },
    ];

    try {
        console.log(`[Worker] Sending message to Slack channel ${CHANNEL_ID} via Bot Token...`);
        await webClient.chat.postMessage({
            channel: CHANNEL_ID,
            text: `Alert Triggered: ${details.name}`, // Fallback text for notifications
            blocks: messageBlocks,
        });
        console.log(`[Worker] Slack notification sent successfully for rule ${details.id}`);
    } catch (error) {
        console.error(`[Worker] Failed to send Slack notification for rule ${details.id}:`, error);
    }
}

module.exports = { sendMessageWithBotToken };
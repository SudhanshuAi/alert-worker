// workers/sendSlackNotification.js
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;

async function sendNotificationToSlack(details) {
    if (!SLACK_WEBHOOK_URL) {
        console.error("[Worker] SLACK_WEBHOOK_URL is not defined in .env.local. Cannot send notification.");
        return;
    }

    const message = {
        text: `🚨 Alert Triggered: ${details.name}`,
        blocks: [
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
            {
                type: 'actions',
                elements: [
                    {
                        type: 'button',
                        text: { type: 'plain_text', text: 'View Alert Dashboard' },
                        // This URL takes the user directly to your application's alert page
                        url: `${process.env.NEXT_PUBLIC_BASE_URL || 'http://localhost:3000'}/alert` 
                    }
                ]
            }
        ],
    };

    try {
        // Using 'fetch', which is built into modern Node.js, avoids extra dependencies
        const response = await fetch(SLACK_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(message),
        });
        if (!response.ok) {
            console.error(`[Worker] Slack API returned an error: ${response.status} ${response.statusText}`);
        } else {
            console.log(`[Worker] Slack notification sent successfully for rule ${details.id}`);
        }
    } catch (error) {
        console.error(`[Worker] Failed to send Slack notification for rule ${details.id}:`, error);
    }
}

module.exports = { sendNotificationToSlack };
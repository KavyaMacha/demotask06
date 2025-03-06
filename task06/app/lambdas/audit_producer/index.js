const AWS = require("aws-sdk");

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const auditTable = process.env.TARGET_TABLE || "Audit";

exports.handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    const auditEntries = event.Records.map((record) => {
        try {
            const { eventName, dynamodb } = record;
            if (!dynamodb.Keys) throw new Error("Missing Keys in DynamoDB event.");

            const itemKey = Object.values(dynamodb.Keys)[0]; // Handles any type (String, Number)
            const modificationTime = new Date().toISOString();

            let auditData = {
                id: AWS.util.uuid.v4(),
                itemKey,
                modificationTime,
            };

            if (eventName === "INSERT") {
                auditData.newValue = dynamodb.NewImage ? AWS.DynamoDB.Converter.unmarshall(dynamodb.NewImage) : null;
            } else if (eventName === "MODIFY") {
                auditData.oldValue = dynamodb.OldImage ? AWS.DynamoDB.Converter.unmarshall(dynamodb.OldImage) : null;
                auditData.newValue = dynamodb.NewImage ? AWS.DynamoDB.Converter.unmarshall(dynamodb.NewImage) : null;
            } else if (eventName === "REMOVE") {
                auditData.deleted = true;
            }

            return { PutRequest: { Item: auditData } };
        } catch (error) {
            console.error("Error processing record:", error);
            return null; // Skip this record
        }
    }).filter(entry => entry !== null); // Remove null entries

    if (auditEntries.length === 0) {
        console.log("No valid audit records to insert.");
        return { statusCode: 200, body: "No records processed" };
    }

    try {
        await dynamoDB.batchWrite({
            RequestItems: { [auditTable]: auditEntries }
        }).promise();
        return { statusCode: 200, body: "Success" };
    } catch (error) {
        console.error("Error writing to Audit table:", error);
        return { statusCode: 500, body: "Error processing DynamoDB stream" };
    }
};

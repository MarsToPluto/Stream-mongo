// mongooseChangeStreamResumable.js
const mongoose = require('mongoose');

// --- CONFIGURATION ---
const uri = "mongodb://localhost:27017/mongooseChangeStreamTestDb?replicaSet=rs0"; // MODIFY THIS
const collectionNameForModel = "MongooseResumableMessage";
// --- END CONFIGURATION ---

// --- Define a Mongoose Schema and Model ---
const messageSchema = new mongoose.Schema({
    message: String,
    sender: String,
    sequence: Number, // Added for easy tracking
    timestamp: { type: Date, default: Date.now }
});

const MessageModel = mongoose.model(collectionNameForModel, messageSchema);

// --- Variable to store the last successfully processed resume token ---
let lastResumeToken = null; // In a real app, you might persist this (e.g., in another DB collection or file)

async function setupChangeStream() {
    console.log(`Attempting to set up change stream. Resuming after token: ${lastResumeToken || 'None (starting fresh)'}`);

    const pipeline = [{ $match: { operationType: 'insert' } }];
    const options = lastResumeToken ? { resumeAfter: lastResumeToken } : {};

    const changeStream = MessageModel.watch(pipeline, options);

    changeStream.on('change', (change) => {
        console.log("\n--- New Document Inserted (Resumable)! ---");
        console.log("Operation Type:", change.operationType);
        console.log("Full Document:", change.fullDocument);
        // IMPORTANT: Store the resume token from this change event
        lastResumeToken = change._id;
        console.log("Updated lastResumeToken to:", lastResumeToken);
        console.log("--------------------------------------------");
    });

    changeStream.on('error', async (error) => {
        console.error("\n--- Mongoose Change Stream Error ---");
        console.error("Error Code:", error.code); // MongoDB error codes can be useful
        console.error("Error Message:", error.message);

        // Check if it's a resumable error.
        // Common resumable error codes:
        // HostUnreachable (6), HostNotFound (7), NetworkTimeout (89), ShutdownInProgress (91),
        // PrimarySteppedDown (189), ExceededTimeLimit (262), NotPrimaryOrSecondary (13436), NotPrimaryNoSecondaryOk (13435)
        // This list is not exhaustive, and driver versions might behave differently.
        // Some drivers/Mongoose might automatically attempt to resume for certain network errors.
        // The key is that if an error occurs, the stream closes. We need to restart it.
        // A simple check for now:
        if (error.codeName === 'ChangeStreamHistoryLost') {
            console.warn("Change stream history lost (oplog rolled over). Restarting stream from scratch.");
            lastResumeToken = null; // Cannot resume, so start fresh
        } else {
            console.warn("Attempting to re-establish change stream after error...");
        }
        // It's good practice to close the current errored stream if it's not already closed implicitly
        if (!changeStream.closed) {
            await changeStream.close().catch(closeErr => console.error("Error closing errored stream:", closeErr));
        }
        // Re-establish the stream after a short delay
        setTimeout(setupChangeStream, 5000); // Retry after 5 seconds
        console.error("------------------------------------");
    });

    changeStream.on('close', () => {
        // This event might be emitted after an error that leads to closure, or if DB connection drops.
        // The 'error' handler above will attempt to re-establish.
        console.log("\n--- Mongoose Change Stream Closed (will attempt to re-establish if due to error) ---");
    });

    console.log(`Watching for new inserts in collection for model "${collectionNameForModel}"...`);
    return changeStream; // Return the stream so it can be closed on app shutdown if needed
}


async function runMongooseResumableDemo() {
    let currentChangeStream = null;
    try {
        await mongoose.connect(uri);
        console.log("Successfully connected to MongoDB via Mongoose.");

        currentChangeStream = await setupChangeStream(); // Initial setup

        console.log("\nTo test resumability:");
        console.log("1. Insert some data.");
        console.log("2. Stop the MongoDB primary node (or simulate a network partition).");
        console.log("3. Observe error and reconnection attempt.");
        console.log("4. Restart the MongoDB primary node.");
        console.log("5. Insert more data after the stream re-establishes.");
        console.log(`\nExample insert (Mongo Shell): use ${mongoose.connection.name}`);
        console.log(`db.${MessageModel.collection.name}.insertOne({ message: "Test Resumable", sequence: 1, sender: "Dev", timestamp: new Date() })`);
        console.log("\nPress Ctrl+C to stop the listener.");

        await new Promise(() => {}); // Keep alive

    } catch (err) {
        console.error("An error occurred during the Mongoose resumable demo:", err);
    } finally {
        console.log("\nInitiating shutdown process...");
        if (currentChangeStream && !currentChangeStream.closed) {
            console.log("Closing active change stream...");
            await currentChangeStream.close().catch(e => console.error("Error closing change stream on exit:", e));
        }
        console.log("Closing Mongoose connection...");
        await mongoose.disconnect();
        console.log("Mongoose connection closed.");
    }
}

// Graceful shutdown for Ctrl+C
const cleanup = async (signal) => {
    console.log(`\nReceived ${signal}. Forcing exit for demo simplicity, but ensuring Mongoose disconnects.`);
    // In a real app, you'd signal the main loop to break and allow finally block to execute
    await mongoose.disconnect().catch(err => console.error("Error during Mongoose disconnect on shutdown signal:", err));
    process.exit(0);
};

process.on('SIGINT', () => cleanup('SIGINT'));
process.on('SIGTERM', () => cleanup('SIGTERM'));

runMongooseResumableDemo().catch(console.error);
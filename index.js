require('dotenv').config(); 
const express = require('express');
const admin = require('firebase-admin');
const serviceAccount = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://handzy-c04d2-default-rtdb.firebaseio.com",
});

const db = admin.database();
const JOBS_REF = '/jobs';
const WORKERS_REF = '/workers';
const PAST_JOBS_REF = '/PastJobs';

/**
 * Notify a specific worker about a job.
 * @param {Object} worker - Worker details.
 * @param {string} jobId - Job ID.
 * @param {Object} jobData - Job data.
 */
async function notifyWorker(worker, jobId, jobData) {
  console.log(`Notifying worker ${worker.uid} about job ${jobId}`);
  try {
    await db.ref(`${WORKERS_REF}/${worker.uid}/jobNotification`).set({
      jobId: jobId,
      status: 'pending',
      jobDetails: jobData,
    });
  } catch (error) {
    console.error(`Error notifying worker ${worker.uid}:`, error);
  }
}

/**
 * Check if a job is expired (older than 12 hours)
 * @param {string} timestamp - Job timestamp
 * @returns {boolean} - True if job is expired
 */
function isJobExpired(timestamp) {
  if (!timestamp) return false;
  const now = new Date();
  const jobTime = new Date(timestamp);
  const hoursDifference = (now - jobTime) / (1000 * 60 * 60);
  return hoursDifference >= 12;
}

/**
 * Move expired job to past jobs.
 * @param {string} jobId - Job ID.
 * @param {Object} jobData - Job data.
 */
async function moveToPastJobs(jobId, jobData) {
  console.log(`Job ${jobId} is expired (older than 12 hours). Moving to past jobs.`);
  await db.ref(`${PAST_JOBS_REF}/${jobId}`).set({
    ...jobData,
    status: 'no worker accepted'
  });
  await db.ref(`${JOBS_REF}/${jobId}`).remove();
}

/**
 * Process workers for a specific job.
 */
async function processWorkers(jobId, jobData) {
  const workersSnapshot = await db.ref(WORKERS_REF).orderByChild('availability').equalTo('available').once('value');
  if (!workersSnapshot.exists()) {
    console.log(`No available workers for job: ${jobId}`);
    await db.ref(`${JOBS_REF}/${jobId}`).update({ status: 'pending' });
    return;
  }

  const workers = [];
  workersSnapshot.forEach((workerSnap) => {
    const workerData = workerSnap.val();
    if (workerData.service === jobData.job) {
      workers.push({ ...workerData, uid: workerSnap.key });
    }
  });

  if (workers.length === 0) {
    console.log(`No available workers with matching service for job: ${jobId}`);
    await db.ref(`${JOBS_REF}/${jobId}`).update({ status: 'pending' });
    return;
  }

  // Sort workers by distance
  workers.sort((a, b) => a.distance - b.distance);
  
  // Process workers in batches of 10
  const BATCH_SIZE = 10;
  let currentBatch = 0;
  let jobAssigned = false;
  
  while (currentBatch * BATCH_SIZE < workers.length && !jobAssigned) {
    const startIdx = currentBatch * BATCH_SIZE;
    const endIdx = Math.min(startIdx + BATCH_SIZE, workers.length);
    const workerBatch = workers.slice(startIdx, endIdx);
    
    console.log(`Processing batch ${currentBatch + 1} (workers ${startIdx + 1}-${endIdx})`);
    
    // Track job assignment status within this batch
    const batchResults = await Promise.all(
      workerBatch.map(async (worker) => {
        const result = await handleWorkerNotification(worker, jobId, jobData);
        return { worker, accepted: result };
      })
    );
    
    // Check if any worker in this batch accepted the job
    const acceptedWorker = batchResults.find(result => result.accepted);
    if (acceptedWorker) {
      jobAssigned = true;
      console.log(`Job assigned to worker ${acceptedWorker.worker.uid} in batch ${currentBatch + 1}`);
    } else {
      console.log(`No workers in batch ${currentBatch + 1} accepted the job. Moving to next batch.`);
      currentBatch++;
    }
  }
  
  if (!jobAssigned) {
    console.log(`No workers accepted job ${jobId} after trying all ${workers.length} available workers.`);
    await db.ref(`${JOBS_REF}/${jobId}`).update({ status: 'unassigned' });
  }
}

/**
 * Handle worker notification, response, and job assignment.
 */
async function handleWorkerNotification(worker, jobId, jobData) {
  console.log(`Notifying worker: ${worker.uid}`);
  await notifyWorker(worker, jobId, jobData);
  const accepted = await waitForWorkerResponse(worker);

  if (accepted) {
    await assignJobToWorker(worker, jobId);
    return true;
  } else {
    console.log(`Worker ${worker.uid} declined the job.`);
    await db.ref(`${WORKERS_REF}/${worker.uid}/jobNotification`).remove();
    return false;
  }
}

/**
 * Simulate waiting for a worker's response.
 * @param {Object} worker - Worker details.
 * @returns {Promise<boolean>} - True if accepted, otherwise false.
 */
function waitForWorkerResponse(worker) {
  return new Promise((resolve) => {
    setTimeout(() => {
      const randomResponse = Math.random() > 0.5; // Simulated response (50% chance)
      resolve(randomResponse);
    }, 15000); // Wait 15 seconds
  });
}

/**
 * Assign a job to a worker using a Firebase transaction.
 */
async function assignJobToWorker(worker, jobId) {
  const jobRef = db.ref(`${JOBS_REF}/${jobId}`);
  const transactionResult = await jobRef.transaction((currentData) => {
    if (currentData && currentData.status === 'pending' && !isJobExpired(currentData.timestamp)) {
      return {
        ...currentData,
        status: 'assigned',
        worker_id: worker.uid,
      };
    }
    return; // Abort transaction if job is no longer pending or is expired
  });

  if (transactionResult.committed) {
    console.log(`Job ${jobId} successfully assigned to worker ${worker.uid}`);
    await db.ref(`${WORKERS_REF}/${worker.uid}`).update({ availability: 'not available' });
    await clearOtherWorkersNotifications(worker.uid, jobId);
  } else {
    console.log(`Job ${jobId} already assigned or expired.`);
  }
}

/**
 * Clear notifications for all other workers once a job is assigned.
 */
async function clearOtherWorkersNotifications(workerUid, jobId) {
  const workersSnapshot = await db.ref(WORKERS_REF).once('value');
  workersSnapshot.forEach((workerSnap) => {
    const workerData = workerSnap.val();
    if (workerSnap.key !== workerUid) {
      db.ref(`${WORKERS_REF}/${workerSnap.key}/jobNotification`).remove();
    }
  });
}

/**
 * Process new jobs (called only when needed).
 */
async function processNewJobs() {
  const startTime = Date.now(); // Record the start time

  const jobsSnapshot = await db.ref(JOBS_REF).orderByChild('status').equalTo('pending').once('value');
  if (!jobsSnapshot.exists()) {
    console.log("No pending jobs to process.");
    return;
  }

  console.log("Processing jobs:", Object.keys(jobsSnapshot.val()).length);

  const jobs = Object.entries(jobsSnapshot.val());

  const workerProcessingPromises = jobs.map(async ([jobId, jobData]) => {
    if (isJobExpired(jobData.timestamp)) {
      await moveToPastJobs(jobId, jobData);
      return;
    }

    await processWorkers(jobId, jobData);
  });

  await Promise.all(workerProcessingPromises); // Parallelize worker processing

  const endTime = Date.now(); // Record the end time
  const elapsedTime = endTime - startTime; // Calculate the time taken
  console.log(`Processed jobs in ${elapsedTime}ms`);
}

// Set up event listener for new jobs instead of interval
let processingInProgress = false;
db.ref(JOBS_REF).on('child_added', async (snapshot) => {
  const jobData = snapshot.val();
  if (jobData.status === 'pending' && !processingInProgress) {
    processingInProgress = true;
    await processNewJobs();
    processingInProgress = false;
  }
});

// Set up periodic check for expired jobs (much less frequent)
setInterval(async () => {
  if (!processingInProgress) {
    processingInProgress = true;
    await processNewJobs();
    processingInProgress = false;
  }
}, 300000); // Check every 5 minutes instead of 10 seconds

// API routes
app.get('/', (req, res) => {
  res.send('Worker assignment service is running');
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Manual trigger endpoint (secured with a simple API key)
app.post('/trigger-job-processing', express.json(), (req, res) => {
  const apiKey = req.headers['x-api-key'];
  if (apiKey !== process.env.API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  if (!processingInProgress) {
    processingInProgress = true;
    processNewJobs().then(() => {
      processingInProgress = false;
    });
    return res.status(202).json({ message: 'Job processing triggered' });
  } else {
    return res.status(409).json({ message: 'Job processing already in progress' });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log("Current Server Time:", new Date().toISOString());
});

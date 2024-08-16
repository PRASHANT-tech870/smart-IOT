const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const firebaseAdmin = require('firebase-admin');

const credentials = require('/serviceAccountKey.json'); // Update with your path

// Initialize Firebase Admin SDK
firebaseAdmin.initializeApp({
    credential: firebaseAdmin.credential.cert(credentials),
    databaseURL: 'https://trail-befa6-default-rtdb.asia-southeast1.firebasedatabase.app/'
});

const db = firebaseAdmin.database();

// Set up Express and Socket.IO
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Serve static files from the public directory
app.use(express.static('public'));

// References to Firebase Realtime Database
const combinedDataRef = db.ref('/CombinedSensorData');

// Fetch initial sensor data
combinedDataRef.once('value')
    .then((snapshot) => {
        const data = snapshot.val();
        io.emit('initial_data', data);
    })
    .catch((error) => {
        console.error("Error fetching data from Firebase:", error);
    });

// Listen for real-time updates
combinedDataRef.on('value', (snapshot) => {
    const data = snapshot.val();
    io.emit('sensor_update', data);
});

// Start the server
const PORT = 3001;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

const express = require('express');
const axios = require('axios');
const http = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.set('view engine', 'ejs');
app.use(express.static('public'));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const API_BASE_URL = 'http://localhost:5080'; // Adjust if backend is on a different port

// Index route - renders the main view
app.get('/', async (req, res) => {
    try {
        const nodesResponse = await axios.get(`${API_BASE_URL}/nodes`);
        const nodes = nodesResponse.data;
        res.render('index', { nodes });
    } catch (error) {
        console.error(error);
        res.status(500).send('Error fetching nodes');
    }
});

app.post('/add', async (req, res) => {
    const { key, value } = req.body;
    try {
        await axios.post(`${API_BASE_URL}/add/${key}`, { value });
        res.redirect('/');
    } catch (error) {
        console.error(error);
        res.status(500).send('Error adding key-value pair');
    }
});

app.get('/get/:key', async (req, res) => {
    const { key } = req.params;
    try {
        const response = await axios.get(`${API_BASE_URL}/get/${key}`);
        res.send(`Value for key ${key}: ${response.data}`);
    } catch (error) {
        console.error(error);
        res.status(500).send('Error retrieving value');
    }
});


// WebSocket connection for real-time updates
io.on('connection', (socket) => {
    console.log('New client connected');
    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});

// Start server
const PORT = 3000;
server.listen(PORT, () => {
    console.log(`Frontend server running on http://localhost:${PORT}`);
});

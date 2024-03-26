const express = require('express');

const app = express();

const PORT = process.env.PORT || 5000;

app.get('/', (req, res) => {
    console.log('HIT BASE ROUTE')
    res.send(200)
})

app.listen(PORT, () => {
    console.log(`SERVER STARTED ON PORT: ${PORT}`);
})

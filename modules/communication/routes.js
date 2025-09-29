var express = require('express');
var app = express();
const path = require('path');

var LOG = require('../egsm-common/auxiliary/logManager');
var egsmengine = require('../egsmengine/egsmengine');

module.id = "ROUTES"

var LOCAL_HOST_NAME = 'localhost' //TODO: retrieve it properly

LOG.logWorker('DEBUG', 'Finding a port to open REST API', module.id)
const MIN_PORT = 8000
const MAX_PORT = 60000
var LOCAL_HTTP_PORT = Math.floor(Math.random() * (MAX_PORT - MIN_PORT + 1) + MIN_PORT);
LOG.logWorker('DEBUG', `Using port ${LOCAL_HTTP_PORT} to open REST API`, module.id)

app.use(express.static(__dirname + '/public'));

app.get('/api/config_stages', function (req, res) {
    var engine_id = req.query.engine_id
    if (typeof engine_id == "undefined") {
        return res.status(500).send({
            error: "No engine engine id provided"
        })
    }
    res.status(200).json(egsmengine.getCompleteDiagram(engine_id));
});

app.get('/api/config_stages_diagram', function (req, res) {
    var engine_id = req.query.engine_id
    if (typeof engine_id == "undefined") {
        return res.status(500).send({
            error: "No engine engine id provided"
        })
    }
    res.status(200).json(egsmengine.getCompleteNodeDiagram(engine_id));
});

app.get('/api/debugLog', function (req, res) {
    var engine_id = req.query.engine_id
    res.json(egsmengine.getDebugLog(engine_id));
});

app.get('/api/infomodel', function (req, res) {
    var engine_id = req.query.engine_id
    if (typeof engine_id == "undefined") {
        return res.status(500).send({
            error: "No engine engine id provided"
        })
    }
    res.status(200).json(egsmengine.getInfoModel(engine_id));
});

app.get('/api/updateInfoModel', function (req, res) {
    var engine_id = req.query.engine_id
    var name = req.query['name']
    var value = req.query['value']
    if (typeof engine_id == "undefined" || typeof name == "undefined" || typeof value == "undefined") {
        return res.status(500).send({
            error: "No engine id/model name / model value provided"
        })
    }
    res.status(200).json(egsmengine.updateInfoModel(engine_id, req.query['name'], req.query['value']));
});

app.get('/api/guards', function (req, res) {
    var engine_id = req.query.engine_id
    if (engine_id == '') {
        res.send('')
        return
    }
    res.json(egsmengine._Guards);
});

app.get('/api/stages', function (req, res) {
    var engineid = req.query.engine_id
    if (engineid == '') {
        res.send('')
        return
    }
    res.json(egsmengine._Stages);
});

app.get('/api/environments', function (req, res) {
    var engineid = req.query.engine_id
    if (engineid == '') {
        res.send('')
        return
    }
    res.json(egsmengine.Environment_);
});

app.get('/api/externals', function (req, res) {
    res.json(egsmengine.Externals_);
});

app.get('*', function (req, res) {
    res.sendFile('index.html', { root: path.join(__dirname, 'public') });
});

const rest_api = app.listen(LOCAL_HTTP_PORT, () => {
    LOG.logWorker(`DEBUG`, `Worker listening on port ${LOCAL_HTTP_PORT}`, module.id)
})

process.on('SIGINT', () => {
    rest_api.close(() => {
        LOG.logWorker(`DEBUG`, `Terminating process...`, module.id)
        process.exit()
    });
});

module.exports = {
    getRESTCredentials: function () {
        return {
            hostname: LOCAL_HOST_NAME,
            port: LOCAL_HTTP_PORT
        }
    }
}
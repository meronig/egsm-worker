/**
 * Module responsible to handle engine-instance-level MQTT communication
 * It handles all messages from stakeholders, performs artifact attach/detach operations, forwards messages to engine instances,
 * and it handles communication with aggregators as well
 */

var xml2js = require('xml2js');
var UUID = require("uuid");

var mqtt = require("../egsm-common/communication/mqttconnector")
var LOG = require('../egsm-common/auxiliary/logManager')
var DB = require("../egsm-common/database/databaseconnector")
var VALIDATOR = require("../egsm-common/database/validator");

module.id = "EVENTR"

let ENGINES = new Map(); //ENGINE_ID -> [DEFAULT_BROKER, onMessageReceived]
let SUBSCRIPTIONS = new Map(); //{HOST, PORT, TOPIC}-> [ENGINE_ID]
let ARTIFACTS = new Map() //ENGINE_ID -> [{ARTIFACT_NAME, BROKER, HOST, BINDING, UNBINDING, ID}]
let STAKEHOLDERS = new Map() //ENGINE_ID -> [{STAKEHOLDER_NAME, PROCESS_ID, BROKER, HOST}]

const TOPIC_PROCESS_LIFECYCLE = 'process_lifecycle'

/**
 * Subscribe an engine specified by its ID to a topic at a specified broker
 * @param {string} engineid 
 * @param {string} topic 
 * @param {string} hostname 
 * @param {int} port 
 * @returns -
 */
function createSubscription(engineid, topic, hostname, port) {
    LOG.logWorker('DEBUG', `createSubscription function called for [${engineid}] to subscribe ${hostname}:${port} -> ${topic}`, module.id)
    var key = [hostname, port, topic].join(":")

    if (!SUBSCRIPTIONS.has(key)) {
        SUBSCRIPTIONS.set(key, [])
    }
    else {
        //Check if the engine is already subscribed to that topic
        if (SUBSCRIPTIONS.has(key) && SUBSCRIPTIONS.get(key).indexOf(engineid) != -1) {
            LOG.logWorker('WARNING', `Engine [${engineid}] is already subscribed to ${hostname}:${port} -> ${topic}`, module.id)
            return
        }
    }

    //Perform subscription
    LOG.logWorker('DEBUG', `Performing subscription: [${engineid}] to ${hostname}:${port} -> ${topic}`, module.id)
    SUBSCRIPTIONS.get(key).push(engineid)
    mqtt.subscribeTopic(hostname, port, topic)
}

/**
 * Delete subscription of a specified engine at a specified broker
 * @param {string} engineid 
 * @param {string} topic 
 * @param {string} hostname 
 * @param {int} port 
 * @returns -
 */
function deleteSubscription(engineid, topic, hostname, port) {
    LOG.logWorker('DEBUG', `deleteSubscription function called for [${engineid}] to unsubscribe ${hostname}:${port} -> ${topic}`, module.id)
    var key = [hostname, port, topic].join(":")

    //Check if subscription exists
    if (!SUBSCRIPTIONS.has(key)) {
        LOG.logWorker('WARNING', `Engine [${engineid}] is not subscribed to ${hostname}:${port} -> ${topic}, cannot be unsubscribed`, module.id)
        return
    }

    //No other engine is subscribed to the topic
    if (SUBSCRIPTIONS.get(key).length == 1) {
        LOG.logWorker('DEBUG', `No other engine subscribed to ${hostname}:${port} -> ${topic}. Performing system level unsubscription`, module.id)
        mqtt.unsubscribeTopic(hostname, port, topic)
        SUBSCRIPTIONS.delete(key)
    }
    //Other engine(s) are still subscribed to the topic
    else {
        for (let i = 0; i < SUBSCRIPTIONS.get(key).length; i++) {
            if (SUBSCRIPTIONS.get(key)[i] == engineid) {
                SUBSCRIPTIONS.get(key).slice(i, 1)
                break
            }
        }
    }
}

/*Required structure of eventDetailsJson:
-Artifact:
    -<artifact_type>:<artifact_id> (artifact_name)
    -<timestamp>
    -<artifact_state> attached/detached
    -<process_type>
    -<process_id>
-Adhoc:
    TODO
    ........
-Stage:
    -<processid>
    -<stagename>
    -<timestamp>
    -<status>
    -<state>
    -<compliance>
*/
/**
 * Publishing logs from engines to the primary broker of the engine
 * @param {string} type 
 * @param {string} engineid 
 * @param {Object} eventDetailsJson 
 */
function publishLogEvent(type, engineid, processtype, processinstance, eventDetailsJson) {
    var topic = engineid
    switch (type) {
        case 'stage':
            if (!VALIDATOR.validateStageLogMessage(eventDetailsJson)) {
                LOG.logWorker('WARNING', `Data is missing to write StageEvent log`, module.id)
                return
            }
            DB.writeStageEvent(eventDetailsJson)
            topic = topic + '/stage_log'
            break;
        case 'condition':
            if (!VALIDATOR.validateConditionLogMessage(eventDetailsJson)) {
                LOG.logWorker('WARNING', `Data is missing to write StageEvent log`, module.id)
                return
            }
            topic = topic + '/stage_log'
            break;
        case 'artifact':
            if (!VALIDATOR.validateArtifactLogMessage(eventDetailsJson)) {
                LOG.logWorker('WARNING', `Data is missing to write ArtifactEvent log`, module.id)
                return
            }
            DB.writeArtifactEvent(eventDetailsJson)
            topic = topic + '/artifact_log'
            break;
        case 'adhoc':
            topic = topic + '/adhoc'
            break;
    }
    mqtt.publishTopic(ENGINES.get(engineid).hostname, ENGINES.get(engineid).port, topic, JSON.stringify(eventDetailsJson))
}

async function publishProcessLifecycleEvent(type, engineid, process_type, instance_id, stakeholders) {
    var message = {
        type: type,
        process: {
            engine_id: engineid,
            stakeholders: stakeholders,
            process_type: process_type,
            instance_id: instance_id,
        }
    }
    mqtt.publishTopic(ENGINES.get(engineid).hostname, ENGINES.get(engineid).port, TOPIC_PROCESS_LIFECYCLE, JSON.stringify(message))
}

/**
 * Messagehandler function
 * Called by the MQTT connector in case of new message arrived and will perform the 
 * necessary subscriptions/unsubscriptions and forward the message to the engine instance too
 * @param {string} hostname 
 * @param {int} port 
 * @param {string} topic 
 * @param {string} message 
 * @returns 
 */
function onMessageReceived(hostname, port, topic, message) {
    LOG.logWorker('DEBUG', `onMessageReceived called`, module.id)
    var key = [hostname, port, topic].join(":")
    if (!SUBSCRIPTIONS.has(key)) {
        LOG.logWorker('DEBUG', `Message received without any subscriber [${hostname}:${port}] :: [${topic}] -> ${message}`, module.id)
        return;
    }

    var elements = topic.split('/')
    var subscribers = SUBSCRIPTIONS.get(key)
    var msgJson = JSON.parse(message.toString())

    for (var engine in subscribers) {
        var stakeholders = STAKEHOLDERS.get(subscribers[engine])
        var artifacts = ARTIFACTS.get(subscribers[engine])
        //Check if the event is from Stakeholder -> do binding/unbinding
        if (elements.length == 2) {
            //Request from aggregator received
            if (elements[1] == 'aggregator_gate') {
                if (msgJson.requesttype == 'stage_duration_compliance_check') {

                }
                if (msgJson.requesttype == 'stage_opening_limit') {

                }
            }
            //Stakeholder message received
            else {
                //Iterate through the engine's skateholders and look for match
                //If a stakeholder found and it is verified that the message is coming from a stakeholder
                //then iterating through the artifacts and their binding and unbinding events
                //Performing subscribe and/or unsubscribe operations if any event match found
                for (var stakeholder in stakeholders) {
                    if ((elements[0] == stakeholders[stakeholder].name) && (elements[1] == stakeholders[stakeholder].instance) && (hostname == stakeholders[stakeholder].host) && (port == stakeholders[stakeholder].port)) {
                        for (var artifact in artifacts) {
                            //Binding events
                            for (var bindigEvent in artifacts[artifact].bindingEvents) {
                                if (msgJson.event.payloadData.eventid == artifacts[artifact].bindingEvents[bindigEvent]) {
                                    //Unsubscribing from old artifact topic if it exists
                                    if (artifacts[artifact].id != '') {
                                        var engineid = subscribers[engine]
                                        deleteSubscription(engineid,
                                            artifacts[artifact].name + '/' + artifacts[artifact].id + '/status',
                                            artifacts[artifact].host,
                                            artifacts[artifact].port)
                                        //Sending event to aggregator
                                        elements = engineid.split('/')
                                        var eventDetail = {
                                            artifact_name: artifacts[artifact].name + '/' + artifacts[artifact].id,
                                            event_id: "event_" + UUID.v4(),
                                            timestamp: Math.floor(Date.now() / 1000),
                                            artifact_state: 'detached',
                                            process_type: elements[0],
                                            process_id: elements[1]
                                        }
                                        publishLogEvent('artifact', engineid, elements[0], elements[1], eventDetail)
                                    }
                                    artifacts[artifact].id = msgJson.event.payloadData.data || ''
                                    if (artifacts[artifact].id != '') {
                                        var engineid = subscribers[engine]
                                        createSubscription(engineid,
                                            artifacts[artifact].name + '/' + artifacts[artifact].id + '/status',
                                            artifacts[artifact].host,
                                            artifacts[artifact].port)
                                        //Sending event to aggregator
                                        elements = engineid.split('/')
                                        var eventDetail = {
                                            artifact_name: artifacts[artifact].name + '/' + artifacts[artifact].id,
                                            event_id: "event_" + UUID.v4(),
                                            timestamp: Math.floor(Date.now() / 1000),
                                            artifact_state: 'attached',
                                            process_type: elements[0],
                                            process_id: elements[1]
                                        }
                                        publishLogEvent('artifact', engineid, elements[0], elements[1], eventDetail)
                                    }
                                }
                            }
                            //Unbinding events
                            for (var unbindigEvent in artifacts[artifact].unbindingEvents) {
                                if (msgJson.event.payloadData.eventid == artifacts[artifact].unbindingEvents[unbindigEvent]) {
                                    if (artifacts[artifact].id != '') {
                                        var engineid = subscribers[engine]
                                        deleteSubscription(engineid,
                                            artifacts[artifact].name + '/' + artifacts[artifact].id + '/status',
                                            artifacts[artifact].host,
                                            artifacts[artifact].port)

                                        //Sending event to aggregator
                                        elements = engineid.split('/')
                                        var eventDetail = {
                                            artifact_name: artifacts[artifact].name + '/' + artifacts[artifact].id,
                                            event_id: "event_" + UUID.v4(),
                                            timestamp: Math.floor(Date.now() / 1000),
                                            artifact_state: 'detached',
                                            process_type: elements[0],
                                            process_id: elements[1]
                                        }
                                        publishLogEvent('artifact', engineid, elements[0], elements[1], eventDetail)
                                        artifacts[artifact].id = ''
                                    }
                                }
                            }
                            ENGINES.get(subscribers[engine]).onMessageReceived(subscribers[engine], JSON.parse(message.toString()).event.payloadData.eventid, '')
                        }
                    }
                }
            }
        }
        //Check if the event is from Artifact and forward it to the engine
        else if (elements.length == 3 && elements[2] == 'status') {
            //Forward message to the engine
            //client.methods.notifyPASO({ parameters: { name: elements[0], value: (JSON.parse(message.toString())).event.payloadData.status, timestamp: (JSON.parse(message.toString())).event.payloadData.timestamp } }, function (data, response) {
            //ENGINES.get(subscribers[engine]).onMessageReceived(subscribers[engine], JSON.parse(message.toString()).event.payloadData.eventid, '')
            ENGINES.get(subscribers[engine]).onMessageReceived(subscribers[engine], elements[0], JSON.parse(message.toString()).event.payloadData.status)
        }
    }
}

/**
 * Set up default broker and onMessageReceived function for a specified engine
 * @param {string} engineid 
 * @param {string} hostname 
 * @param {int} port 
 * @param {function} onMessageReceived 
 */
function setEngineDefaults(engineid, hostname, port, onMessageReceived) {
    LOG.logWorker('DEBUG', `setDefaultBroker called: ${engineid} -> ${hostname}:${port}`, module.id)
    ENGINES.set(engineid, { hostname: hostname, port: port, onMessageReceived: onMessageReceived })
}

/**
 * Init connections for a specified engine based on the provided binding file
 * @param {string} engineid 
 * @param {string} bindingfile 
 * @returns "ok" if the process was successful, "error" otherwise
 */
function initConnections(engineid, bindingfile) {
    const elements = engineid.split('__')
    const elements2 = elements[0].split('/')
    var instance_id = elements2[1]

    LOG.logWorker('DEBUG', `initConnections called for ${engineid}`, module.id)
    var parseString = xml2js.parseString;
    return parseString(bindingfile, function (err, result) {
        if (err) {
            LOG.logWorker('ERROR', `Error while parsing biding file for ${engineid}: ${err}`, module.id)
            return 'error'
        }
        //Read Artifacts
        //Check if the engine id is defined in the map as a key
        if (!ARTIFACTS.has(engineid)) {
            ARTIFACTS.set(engineid, [])
        }

        //Iterate through artifacts and add them to the map
        var ra = result['martifact:definitions']['martifact:mapping'][0]['martifact:artifact'];
        for (var artifact in ra) {
            var br = [];
            for (var aid in ra[artifact]['martifact:bindingEvent']) {
                br.push(ra[artifact]['martifact:bindingEvent'][aid]['$'].id);
            }
            var ur = [];
            for (var aid in ra[artifact]['martifact:unbindingEvent']) {
                ur.push(ra[artifact]['martifact:unbindingEvent'][aid]['$'].id);
            }
            ARTIFACTS.get(engineid).push({
                name: ra[artifact]['$'].name, bindingEvents: br,
                unbindingEvents: ur,
                host: ra[artifact]['$'].broker_host || ENGINES.get(engineid).hostname,
                port: ra[artifact]['$'].port || ENGINES.get(engineid).port,
                id: ''
            })
        }

        // Read Stakeholders
        //Check if the engine id is defined in the map as a key
        if (!STAKEHOLDERS.has(engineid)) {
            STAKEHOLDERS.set(engineid, [])
        }

        //Iterate through stakeholders and add them to the map
        var stakeHolders = result['martifact:definitions']['martifact:stakeholder'];
        for (var key in stakeHolders) {
            STAKEHOLDERS.get(engineid).push({
                name: stakeHolders[key]['$'].name,
                instance: instance_id,
                host: stakeHolders[key]['$'].broker_host || ENGINES.get(engineid).hostname,
                port: stakeHolders[key]['$'].port || ENGINES.get(engineid).port
            })
            createSubscription(engineid,
                stakeHolders[key]['$'].name + '/' + instance_id,
                stakeHolders[key]['$'].broker_host || ENGINES.get(engineid).hostname,
                stakeHolders[key]['$'].port || ENGINES.get(engineid).port);
        }

        //Subscribe to a special topic to 'open' a communication interfaces for Aggregator Agent(s)
        createSubscription(engineid, engineid + '/aggregator_gate', stakeHolders[key]['$'].broker_host
            || ENGINES.get(engineid).hostname, stakeHolders[key]['$'].port || ENGINES.get(engineid).port)
        return 'ok'
    })
}

/**
 * Should be called by the engine instance before termination
 * The function will unsubscribe from all related topics and remove the process from the module
 * @param {string} engineid 
 * @returns "ok" or "not_defined" if the engine is not defined in this module
 */
function onEngineStop(engineid) {
    if (ENGINES.has(engineid)) {
        var topics = []
        SUBSCRIPTIONS.forEach(function (value, key) {
            for (i in value) {
                if (value[i] == engineid) {
                    topics.push(key)
                    break
                }
            }
        })
        for (i in topics) {
            var elements = topics[i].split(':')
            deleteSubscription(engineid, elements[2], elements[0], elements[1])
        }
        ENGINES.delete(engineid)
        return 'ok'
    }
    else {
        return 'not_defined'
    }
}

//Adding reference to onMessageReceived to the mqtt module to receive notifications
mqtt.init(onMessageReceived)

module.exports = {
    publishLogEvent: publishLogEvent,
    publishProcessLifecycleEvent: publishProcessLifecycleEvent,
    setEngineDefaults: setEngineDefaults,
    initConnections: initConnections,
    onEngineStop: onEngineStop,
};

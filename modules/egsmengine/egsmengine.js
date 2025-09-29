var xml2js = require('xml2js');

var DDB = require('../egsm-common/database/databaseconnector')
const EVENTR = require('../eventrouter/eventrouter');
var EventManager = require('../egsm-common/auxiliary/eventManager');
var LOG = require("../egsm-common/auxiliary/logManager")
const { Validator } = require('../egsm-common/auxiliary/validator');
var CONNCONFIG = require('../egsm-common/config/connectionconfig')

//===============================================================DATA STRUCTURES BEGIN=========================================================================
var MAX_ENGINES = 50000
var ENGINES = new Map();

//Data structures to calculate unique, sequentual event ID-s for each stage events
var STAGE_EVENT_ID = new Map()

function Engine(id, processstakeholders) {
    const engineid = id; //Engine ID structure must be: <PROCESS_TYPE>/<INSTANCE_ID>__<PERSPECTIVE> (e.g.: "TRANSPORTATION/instance_1__Truck")
    const elements = id.split('__')
    const elements2 = elements[0].split('/')
    const process_type = elements2[0]
    const process_instance = elements2[1]
    const process_perspective = elements[1]
    const stakeholders = processstakeholders
    const startTime = new Date().getTime() / 1000 / 60
    var lifeCycleStatus = "RUNNING" //RUNNING/FINISHED

    // initialize arrays containing process model elements
    var Data_array = {};        //data flow guards, process flow guards, fault loggers and milestones
    var Stage_array = {};       //stages
    var Info_array = {};        //information model
    var Event_array = {};    //events
    var Dependency_Array = {};  //dependencies
    var eventManager = new EventManager.EventManager(engineid);

    initModel = function (processModel, infoModel) {
        //reset arrays
        this.Data_array = {};
        this.Stage_array = {};
        this.Dependency_Array = {};
        this.Info_array = {};
        this.Event_array = {};

        //reset event manager
        this.eventManager.reset();

        //parse XML, section 'ca:EventModel'
        var events = processModel['ca:CompositeApplicationType']['ca:EventModel'][0]['ca:Event'];
        for (var key in events) {
            PARSER.eventParsing(engineid, events[key]);
        }

        //parse XML, section 'ca:Stage'
        var stages = processModel['ca:CompositeApplicationType']['ca:Component'][0]['ca:GuardedStageModel'][0]['ca:Stage'];
        for (var key in stages) {
            PARSER.stageParsingRecursive(engineid, stages[key], 0, '');
        }

        //configure listener for DATA events
        PARSER.setDataListeners(engineid);

        //parse XSD, section 'xs:element'
        var infos = infoModel['xs:schema']['xs:element'];
        for (var key in infos) {
            PARSER.infoParsing(engineid, infos[key]);
        }

        //update DATA, EVENT, STAGE, INFORMATION instances with dependencies computed by PARSER
        PARSER.setDependencies(engineid);

        //initialize process by evaluating all sentries for the first time
        for (key in this.Data_array) {
            this.Data_array[key].update(false);
        }

        //uncomment to enable configuration of communication manager
        //CommunicationManager.init(Info_array);
    }

    var reset = function () {
        //reset arrays
        this.Data_array = {};
        this.Stage_array = {};
        this.Dependency_Array = {};
        this.Info_array = {};
        this.Event_array = {};
        //reset event manager
        this.eventManager.reset();
    }

    var updateInfoModel = function (name, value) {
        var attrs = [];
        if (value != undefined && value != '') {
            attrs = [];
            attrs[0] = new Object();
            attrs[0].name = 'status';
            attrs[0].value = value;
            this.Info_array[name].changeAttributes(attrs);
        }
        else {
            this.Info_array[name].changeAttributes(attrs);
        }

    }

    // E-GSM process instance: used to evaluate expressions and PAC rules
    var GSM = {
        engineid: engineid,
        // check if a milestone is achieved
        isMilestoneAchieved: function (milestone) {
            return ENGINES.get(this.engineid).Data_array[milestone].value;
        },
        // check if an event is occurring
        isEventOccurring: function (event) {
            return ENGINES.get(this.engineid).Event_array[event].value;
        },
        // check if a stage is active (opened)
        isStageActive: function (stage) {
            return ENGINES.get(this.engineid).Stage_array[stage].state == 'opened';
        },
        // check information model
        isInfoModel: function (model, attribute, value, operator) {
            // EXPERIMENTAL: learn admissibile attribute values
            var attributes = ENGINES.get(this.engineid).Info_array[model]._attributes;
            for (var key in attributes) {
                if (attributes[key].name == attribute && attributes[key]._learningValues.indexOf(value) < 0) {
                    // add attribute value to list if not already present
                    attributes[key]._learningValues.push(value);
                }
            }

            // check information model status
            var checkValue = false;
            for (var key in attributes) {
                if (attributes[key].name == attribute) {
                    switch (operator) {
                        case "==":
                            // value is identical to specified one
                            if (attributes[key].value == value)
                                checkValue = true;
                            break;
                        case "!=":
                            // value is identical to specified one
                            if (attributes[key].value != value)
                                checkValue = true;
                            break;
                        case "<=":
                            // value is identical to specified one
                            if (attributes[key].value <= value)
                                checkValue = true;
                            break;
                        case ">=":
                            // value is identical to specified one
                            if (attributes[key].value >= value)
                                checkValue = true;
                            break;
                        case "<":
                            // value is identical to specified one
                            if (attributes[key].value < value)
                                checkValue = true;
                            break;
                        case ">":
                            // value is identical to specified one
                            if (attributes[key].value > value)
                                checkValue = true;
                            break;
                    }
                }
            }
            return checkValue;
        },
        // evaluate expressions (sentries)
        eval: function (expression) {
            try {
                // return results of the expression
                return eval(expression);
            } catch (e) {
                // Log exception
                console.log(`eval err: ${expression}`)
                //LogManager.logWorker(`ERROR`, `Sentry evaluation error: ${expression}`)
                // if an exception is raised, always return 'false'
                return false;
            }
        }
    };

    //class to convert internal models for UI representation
    var UI = {
        engineid: engineid,
        //recursive method to build JSON file for UI (convert flat array into hierarchical structure)
        completeDiagram: function (json, stage, rank) {
            if (stage) {
                //define JSON fields
                json.name = ENGINES.get(this.engineid).Stage_array[stage.name].name;
                json.state = ENGINES.get(this.engineid).Stage_array[stage.name].state;
                json.status = ENGINES.get(this.engineid).Stage_array[stage.name].status;
                json.compliance = ENGINES.get(this.engineid).Stage_array[stage.name].compliance;
                json.array_dep = ENGINES.get(this.engineid).Stage_array[stage.name]._array_dep;
                //populate data flow guards
                json.dataGuards = [];
                for (var key2 in stage._dataGuards) {
                    json.dataGuards.push(ENGINES.get(this.engineid).Data_array[stage._dataGuards[key2]]);
                }
                //populate process flow guards
                json.processGuards = [];
                for (var key2 in stage._processGuards) {
                    json.processGuards.push(ENGINES.get(this.engineid).Data_array[stage._processGuards[key2]]);
                }
                //populate milestones
                json.milestones = [];
                for (var key2 in stage._milestones) {
                    json.milestones.push(ENGINES.get(this.engineid).Data_array[stage._milestones[key2]]);
                }
                //populate fault loggers
                json.faults = [];
                for (var key2 in stage._faults) {
                    json.faults.push(ENGINES.get(this.engineid).Data_array[stage._faults[key2]]);
                }
                //add child stages
                json.sub_stages = [];
                for (var key in stage._childs) {
                    if (typeof ENGINES.get(this.engineid).Stage_array[stage._childs[key]] == "object") {
                        //invoke recursive function
                        json.sub_stages.push(UI.completeDiagram({}, ENGINES.get(this.engineid).Stage_array[stage._childs[key]], ENGINES.get(this.engineid).Stage_array[stage._childs[key]].rank));
                    }
                }
                return json;
            }
            return json;
        }
    }

    //Exposed Engine functions
    return {
        engineid: engineid,
        process_type: process_type,
        process_instance: process_instance,
        process_perspective: process_perspective,
        startTime: startTime,
        lifeCycleStatus: lifeCycleStatus,

        Data_array: Data_array,
        Stage_array: Stage_array,
        Info_array: Info_array,
        Event_array: Event_array,
        Dependency_Array: Dependency_Array,
        eventManager: eventManager,
        GSM: GSM,
        UI: UI,
        initModel: initModel,
        reset: reset,
        updateInfoModel: updateInfoModel
    }
}

// attribute model
var ATTRIBUTES = {
    // initialize instance
    init: function (engineid, name, parent, type, use) {
        this.engineid = engineid; // ID of the engine the ATTRIBUTES object belongs to
        this.name = name;
        this.parent = parent;
        this.type = type;
        this.use = use;
        this._learningValues = [];
        this.timestamp = Date.now();

        //TODO: handle attribute 'timestamp', which is different from 'this.timestamp'
        if (name == 'timestamp' && type == 'xs:dateTime') {
            this.value = Date.now();
        }
        else {
            this.value = '';
        }
    },
    // change attribute value
    changeValue: function (newValue) {
        if (this.value != newValue) {
            this.value = newValue;
            this.timestamp = Date.now();
        }
    }
};

// information model
var INFORMATION = {
    // initialize instance
    init: function (engineid, name, pub, sub) {
        this.engineid = engineid; //ID of the engine the INFORMATION object belongs to
        this.name = name;
        this.pub = pub;
        this.sub = sub;
        this._attributes = [];
        this._array_dep = [];
    },
    // change attributes
    changeAttributes: function (newAttributes) {
        var changedValue = false;
        var changedStatus = false;
        for (var key in newAttributes) {
            var attr = this._attributes[newAttributes[key].name];
            if (attr != undefined) {
                // attribute found
                if (attr.value != newAttributes[key].value) {
                    changedValue = true;
                    // update value if different from current one, and emit 'name_l' and 'name_e' events
                    attr.changeValue(newAttributes[key].value);
                    if (attr.name == 'status') {
                        changedStatus = true;
                        newValue = newAttributes[key].value;
                    }
                }
            }
        }

        //TODO: revise if XML or XSD file structure changes
        //events 'name_l' and 'name_e' are emitted only if 'status' attribute is present in event payload, otherwise only event 'name' is emitted.
        if (changedValue && changedStatus) {
            if (ENGINES.get(this.engineid).Event_array[this.name + '_l'] != undefined && ENGINES.get(this.engineid).Event_array[this.name + '_e'] != undefined) {
                //emit events 'name_l' e 'name_e'
                ENGINES.get(this.engineid).Event_array[this.name + '_l'].emitEvent();
                ENGINES.get(this.engineid).Event_array[this.name + '_e'].emitEvent();

                //handle process flow guard dependencies
                for (var item in this._array_dep) {
                    if (ENGINES.get(this.engineid).Data_array[this._array_dep[item]].type == 'P') {
                        ENGINES.get(this.engineid).Data_array[this._array_dep[item]].update(false);
                    }
                }
            }
        }
        else if (!changedValue && !changedStatus) {
            //emit event 'name'
            if (ENGINES.get(this.engineid).Event_array[this.name] != undefined) {
                ENGINES.get(this.engineid).Event_array[this.name].emitEvent();
            }
        }
    }
};

// event model
var EVENT = {
    // initialize instance
    init: function (engineid, name) {
        this.engineid = engineid;
        this.name = name;
        //event status: if true then emit event and re-evaluate model
        this.value = false;
        this._array_dep = [];
        this.timestamp = Date.now();
    },
    // emit event
    emitEvent: function () {
        //true -> event was received
        this.setActive();
        ENGINES.get(this.engineid).eventManager.emit(this.name, ENGINES.get(this.engineid).Event_array[this.name]._array_dep);
        this.setUnactive();
        //false -> event was handled (so to disable guard after evaluating sentry)
        ENGINES.get(this.engineid).eventManager.emit(this.name, ENGINES.get(this.engineid).Event_array[this.name]._array_dep);
    },
    // activate event status (when event is received)
    setActive: function () {
        this.timestamp = Date.now();
        this.value = true;
    },
    // deactivate event status (when event is handled)
    setUnactive: function () {
        this.timestamp = Date.now();
        this.value = false;
    }
};

//data model (data flow guards, process flow guards, fault loggers e milestones)
var DATA = {
    //initialize instance
    init: function (engineid, name, stage, sentry, type) {
        this.engineid = engineid;
        this.name = name;
        this.value = false;
        this.stage = stage;
        this.sentry = sentry;
        this.type = type;
        this._array_dep = [];
        this.timestamp = Date.now();
        //Check
        if (sentry == undefined) { sentry = 'false'; }
    },
    //evaluate sentry (invoked by the engine when a dependency between the sentry and an element that changed is detected)
    update: function (invalidator) {
        var oldValue = this.value;
        var newValue = this.value;
        //here
        //LogManager.logModelData(this.name, this.type, this.stage, 'UPDATE START', this.value, this.sentry);

        //evaluate sentry
        if (this.type == 'D') //Data flow guard
        {
            newValue = ENGINES.get(this.engineid).GSM.eval(this.sentry);
        }
        else if (this.type == 'P') //Process flow guard
        {
            newValue = ENGINES.get(this.engineid).GSM.eval(this.sentry);
        }
        else if (this.type == 'M') //Milestone
        {
            //milestone is evaluated only when the stage is open
            if (ENGINES.get(this.engineid).Stage_array[this.stage].state == 'opened') {
                newValue = ENGINES.get(this.engineid).GSM.eval(this.sentry);
            }
            else if (invalidator == true) {
                //invalidate milestone
                newValue = false;
            }
        }
        else if (this.type == 'F') //Fault logger
        {
            //fault logger is evaluated only if the stage is open and its value is 'false'
            if (ENGINES.get(this.engineid).Stage_array[this.stage].state == 'opened' && this.value == false) {
                newValue = ENGINES.get(this.engineid).GSM.eval(this.sentry);
            }
        }

        //check if value has changed after evaluating sentry
        if (oldValue != newValue) {
            this.value = newValue;
            this.timestamp = Date.now();
            if (this.type != 'P') {
                //TODO check if exclusion of process flow guards is correct
                var dep = [this.stage];
                if (this._array_dep.length > 0)
                    Array.prototype.push.apply(dep, this._array_dep);
                ENGINES.get(this.engineid).eventManager.emit(this.name, dep);
            } else {
                var engine = ENGINES.get(this.engineid)
                var eventJson = {
                    process_type: engine.process_type,
                    process_id: engine.process_instance,
                    process_perspective: engine.process_perspective,
                    stage_name: this.stage,
                    timestamp: Date.now(),
                    condition: newValue
                }
                EVENTR.publishLogEvent('condition', this.engineid, engine.process_type, engine.process_instance, eventJson)
            }
        }
    }
};

//stage model
var STAGE = {
    init: function (engineid, name, parent, rank) {
        this.engineid = engineid;
        this.name = name;
        this.status = 'regular'; //possible values: 'regular', 'faulty'
        this.state = 'unopened'; //possible values 'unopened', 'opened', 'closed'
        this.compliance = 'onTime'; //possible values: 'onTime', 'outOfOrder', 'skipped'
        this.parent = parent;
        this.rank = rank;
        this.timestamp = Date.now();
        this._array_dep = [];
        this._dataGuards = [];
        this._processGuards = [];
        this._milestones = [];
        this._faults = [];
        this._childs = [];
        this._history = [];
        this._pendingStateChange = false; //flag to track pending state changes
    },

    //reset model
    reset: function (resetStage) {
        if (resetStage) {
            this._beginStateChanges();
            this._changeState('unopened');
            this._changeStatus('regular');
            this._changeCompliance('onTime');
            this._commitStateChanges();
        }
        //recursively reset all child stages
        for (var ch in this._childs) {
            ENGINES.get(this.engineid).Stage_array[this._childs[ch]].reset(true);
        }
        //invalidate all milestones for current stage
        for (var mile in ENGINES.get(this.engineid).Data_array) {
            if (ENGINES.get(this.engineid).Data_array[mile].stage == this.name && ENGINES.get(this.engineid).Data_array[mile].type == 'M') {
                console.log('Invalidazione --> ' + mile);
                ENGINES.get(this.engineid).Data_array[mile].update(true);
            }
        }
        //re-compute all process flow guards for current stage
        for (var proc in ENGINES.get(this.engineid).Data_array) {
            if (ENGINES.get(this.engineid).Data_array[proc].stage == this.name && ENGINES.get(this.engineid).Data_array[proc].type == 'P') {
                console.log('Reset process guards --> ' + proc);
                ENGINES.get(this.engineid).Data_array[proc].update(true);
            }
        }
    },

    logStageState: function () {
        var eventid = STAGE_EVENT_ID.get(this.engineid) + 1;
        STAGE_EVENT_ID.set(this.engineid, eventid);

        const elements = this.engineid.split('__');
        const elements2 = elements[0].split('/');
        const process_type = elements2[0];
        const process_instance = elements2[1];
        const process_perspective = elements[1];

        var eventJson = {
            process_type: process_type,
            process_id: process_instance,
            process_perspective: process_perspective,
            event_id: 'event_' + eventid.toString(),
            stage_name: this.name,
            timestamp: Date.now(),
            status: this.status,
            state: this.state,
            compliance: this.compliance,
            whole: ENGINES.get(this.engineid).Stage_array
        };
        EVENTR.publishLogEvent('stage', this.engineid, process_type, process_instance, eventJson);
    },

    //begin a batch of state changes
    _beginStateChanges: function() {
        this._pendingStateChange = true;
    },

    //commit state changes and emit event - only once per update
    _commitStateChanges: function() {
        if (this._pendingStateChange) {
            this._pendingStateChange = false;
            this.logStageState();
        }
    },

    //internal state change methods that don't emit events
    _changeState: function (newState) {
        var oldState = this.state;
        this.state = newState;
        this.timestamp = Date.now();
        var rev = {};
        rev.timestamp = this.timestamp;
        rev.oldValue = oldState;
        rev.newValue = newState;
        this._history.push(rev);
    },

    _changeCompliance: function (newCompliance) {
        var oldCompliance = this.compliance;
        this.compliance = newCompliance;
        this.timestamp = Date.now();
        var rev = {};
        rev.timestamp = this.timestamp;
        rev.oldValue = oldCompliance;
        rev.newValue = newCompliance;
        this._history.push(rev);
    },

    _changeStatus: function (newStatus) {
        var oldStatus = this.status;
        this.status = newStatus;
        this.timestamp = Date.now();
        var rev = {};
        rev.timestamp = this.timestamp;
        rev.oldValue = oldStatus;
        rev.newValue = newStatus;
        this._history.push(rev);
    },

    //public methods that manage state changes with event emission
    changeState: function (newState) {
        this._beginStateChanges();
        this._changeState(newState);
        this._commitStateChanges();
        
        //reset stage if re-opened
        if (oldState == 'closed' && (newState == 'opened' || newState == 'unopened')) {
            this.reset(false);
        }
    },
    
    changeCompliance: function (newCompliance) {
        this._beginStateChanges();
        this._changeCompliance(newCompliance);
        this._commitStateChanges();
    },
    
    changeStatus: function (newStatus) {
        this._beginStateChanges();
        this._changeStatus(newStatus);
        this._commitStateChanges();
    },

    //verify if a stage should be opened (if it should transition from 'Unopened' to 'Opened')
    checkUnopenedToOpened: function () {
        //parent stage must be opened (or undefined -> no parent exists)
        var checkParent = false;
        if (ENGINES.get(this.engineid).Stage_array[this.parent] != undefined && ENGINES.get(this.engineid).Stage_array[this.parent].state == 'opened') {
            checkParent = true;
        }
        else if (ENGINES.get(this.engineid).Stage_array[this.parent] == undefined) {
            checkParent = true;
        }

        //at least one data flow guard must be active
        var checkData = false;
        for (var i = 0; i < this._dataGuards.length; i++) {
            checkData = checkData || ENGINES.get(this.engineid).Data_array[this._dataGuards[i]].value;
        }

        //return the result
        return checkData && checkParent;
    },
    //verify if a stage should become out of order (if it should transition from 'OnTime' to 'OutOfOrder')
    checkOnTimeOutOfOrder: function () {
        //check process flow guards (if at least one is active, then the transition must NOT fire)
        var checkProcess = false;
        if (this._processGuards.length == 0) {
            checkProcess = true;
        }
        for (var i = 0; i < this._processGuards.length; i++) {
            checkProcess = checkProcess || ENGINES.get(this.engineid).Data_array[this._processGuards[i]].value;
        }

        return !checkProcess;
    },
    //verify if a stage should become faulty (if it should transition from 'Regular' to 'Faulty')
    checkRegularToFaulty: function () {
        var checkFault = false;
        for (var i = 0; i < this._faults.length; i++) {
            checkFault = checkFault || ENGINES.get(this.engineid).Data_array[this._faults[i]].value;
        }
        return checkFault;
    },
    //verify if a stage should be closed (if it should transition from 'Opened' to 'Closed')
    checkOpenedToClosed: function () {
        //verify if parent stage is closed (if so then current stage should be closed too)
        var checkParent = false;
        if (ENGINES.get(this.engineid).Stage_array[this.parent] != undefined && ENGINES.get(this.engineid).Stage_array[this.parent].state == 'closed') {
            checkParent = true;
        }

        //at least one milestone for current stage is fulfilled
        var checkMilestone = false;
        for (var i = 0; i < this._milestones.length; i++) {
            checkMilestone = checkMilestone || ENGINES.get(this.engineid).Data_array[this._milestones[i]].value;
        }

        return checkMilestone || checkParent;
    },
    //verify if a stage should be re-opened (if it should transition from 'Closed' to 'Opened')
    checkClosedToOpened: function () {
        //parent stage must be opened (or undefined -> no parent exists)
        var checkParent = false;
        if (ENGINES.get(this.engineid).Stage_array[this.parent] != undefined && ENGINES.get(this.engineid).Stage_array[this.parent].state == 'opened') {
            checkParent = true;
        }
        else if (ENGINES.get(this.engineid).Stage_array[this.parent] == undefined) {
            checkParent = true;
        }

        //at least one data flow guard must be active
        var checkData = false;
        for (var i = 0; i < this._dataGuards.length; i++) {
            checkData = checkData || ENGINES.get(this.engineid).Data_array[this._dataGuards[i]].value;
        }
        return checkData && checkParent;
    },
    //verify which stages should be marked as skipped (if current stage is open and 'OutOfOrder', determine which stages should transition from 'Regular' to 'Skipped')
    setUnopenedOnTimeRegularToSkipped: function () {
        //check all stages in the model
        for (var s in ENGINES.get(this.engineid).Stage_array) {
            //select only stages that differ from current one with 'UnOpened' status, and 'OnTime' and 'Regular' compliance
            var stage = ENGINES.get(this.engineid).Stage_array[s];
            if (stage.name != this.name && stage.state == 'unopened' && stage.compliance == 'onTime' && stage.status == 'regular') {
                //check all process flow guards for current stage
                for (var p in this._processGuards) {
                    //check if the sentry for current stage contains 'GSM.isStageActive(stage)': if so, then stage 'stage' must be 'skipped'
                    if (ENGINES.get(this.engineid).Data_array[this._processGuards[p]].sentry.indexOf("GSM.isStageActive(\"" + stage.name + "\")") > -1) {
                        stage.changeCompliance('skipped');
                    }
                    else {
                        //check if the sentry for current stage contains 'isMilestoneAchieved(m)', and if m belongs to 'stage': if so, then stage 'stage' must be 'skipped'
                        for (var m in stage._milestones) {
                            //TODO, search for the whole PAC rule (currently only the presence of m in the sentry is checked)
                            if (stage._milestones[m] != null && ENGINES.get(this.engineid).Data_array[this._processGuards[p]].sentry.indexOf(stage._milestones[m]) > -1) {
                                stage.changeCompliance('skipped');
                            }
                        }
                    }
                }
            }
        }
    },
    //update the lifecycle of the stage (called by the engine when a dependency between the current stage and another elment that changed is detected)
    update: function () {
        this._beginStateChanges(); //start batch of state changes
        var oldState = this.state;
        
        //if stage is unopened
        if (this.state == 'unopened') {
            //determine if it should be opened
            if (this.checkUnopenedToOpened()) {
                //open stage
                this._changeState('opened');
                //check compliance (execution order)
                if (this.compliance == 'onTime' && this.checkOnTimeOutOfOrder()) {
                    //incorrect execution order
                    this._changeCompliance('outOfOrder');
                    //find if there were stages that were skipped
                    this.setUnopenedOnTimeRegularToSkipped();
                }
                else if (this.compliance == 'skipped') {
                    //if the stage was 'skipped', then it will always be 'outOfOrder'
                    this._changeCompliance('outOfOrder');
                    this.setUnopenedOnTimeRegularToSkipped();
                }
            }
        }
        //if stage is opened
        else if (this.state == 'opened') {
            //check if it has become faulty
            if (this.status == 'regular' && this.checkRegularToFaulty()) {
                this._changeStatus('faulty');
            }
            //check if it must be closed
            if (this.checkOpenedToClosed()) {
                this._changeState('closed');
            }
        }
        //if stage is closed
        else if (this.state == 'closed') {
            //check if it must be reopened
            if (this.checkClosedToOpened()) {
                //evaluate compliance before resetting stage (and inner elements)
                var checkOnTimeOutOfOrder_checkClosedToOpened = this.checkOnTimeOutOfOrder();
                this._changeState('opened');
                if (this.compliance == 'onTime' && checkOnTimeOutOfOrder_checkClosedToOpened) {
                    //if compliance was not met, then set stage as 'outOfOrder' 
                    this._changeCompliance('outOfOrder');
                    this.setUnopenedOnTimeRegularToSkipped();
                }
            }
        }
        
        //only commit if state actually changed
        this._commitStateChanges();
        
        //fix for reset logic when reopening
        if (oldState == 'closed' && (this.state == 'opened' || this.state == 'unopened')) {
            this.reset(false);
        }

        //handle closing of all child stages if current stage is closed
        var dep = [];
        //add to dependencies all child stages only if current stage is closed (so to close them if still opened)
        //WARNING: do not remove condition [this.state == 'closed'] otherwise closed stages will be reopened
        if (this.state == 'closed' && this._childs.length > 0)
            Array.prototype.push.apply(dep, this._childs);
        if (this._array_dep.length > 0)
            Array.prototype.push.apply(dep, this._array_dep);
        ENGINES.get(this.engineid).eventManager.emit(this.name, dep);
    }
};

//parser model
var PARSER = {
    //convert expressions specified for sentries in the model in a format understandable by the engine
    convertExpressionToSentry: function (engineid, expr, artifactId) {
        //add double quotes to arguments in PAC rules, so to turn them into function parameters (method(argument) --> method("argument"))
        var reg = new RegExp(/\((\w+)\)/g);
        var sentry = expr;
        var variable = '';
        while ((result = reg.exec(sentry)) !== null) {
            variable = result[1];
            sentry = sentry.replace(/\((\w+)\)/, "(\"$1\")");
            //check for dependencies and populate array (which is then used to populate the internal data structure)
            if (ENGINES.get(engineid).Dependency_Array[artifactId] == undefined && variable != '') {
                ENGINES.get(engineid).Dependency_Array[artifactId] = [];
                ENGINES.get(engineid).Dependency_Array[artifactId].push(variable);
            }
            else {
                if (variable != '')
                    ENGINES.get(engineid).Dependency_Array[artifactId].push(variable);
            }
        }

        //replace XPath expression in infoModel with PAC rule GSM.isInfoModel(arg1, arg2, arg3)
        reg = new RegExp(/\{infoModel\.\/infoModel\/(\w+)\/(\w+)\} ([!<>'=]=?) \[(\w+)\]/g);
        while ((result = reg.exec(sentry)) !== null) {
            variable = result[1];
            sentry = sentry.replace(/\{infoModel\.\/infoModel\/(\w+)\/(\w+)\} ([!<>'=]=?) \[(\w+)\]/, "GSM.isInfoModel(\"$1\",\"$2\",\"$4\",\"$3\")");
            //check for dependencies and populate array (which is then used to populate the internal data structure)
            if (ENGINES.get(engineid).Dependency_Array[artifactId] == undefined && variable != '') {
                ENGINES.get(engineid).Dependency_Array[artifactId] = [];
                ENGINES.get(engineid).Dependency_Array[artifactId].push(variable);
            }
            else {
                if (variable != '')
                    ENGINES.get(engineid).Dependency_Array[artifactId].push(variable);
            }
        }

        //replace 'and' with '&&'
        reg = new RegExp(/ and /g);
        while ((result = reg.exec(sentry)) !== null) {
            variable = result[0];
            sentry = sentry.replace(/ and /, " && ");
        }
        //replace 'or' with '||'
        reg = new RegExp(/ or /g);
        while ((result = reg.exec(sentry)) !== null) {
            variable = result[0];
            sentry = sentry.replace(/ or /, " || ");
        }
        //replace 'not' with '!'
        reg = new RegExp(/((not )|( not))/g);
        while ((result = reg.exec(sentry)) !== null) {
            variable = result[0];
            sentry = sentry.replace(/((not )|( not))/, "!");
        }
        //return expression converted to an executable sentry
        return sentry;
    },
    //recursive function to parse XML file and create internal data structure
    stageParsingRecursive: function (engineid, nextStage, rank, parent) {
        //for each stage at root level, invoke recursive function to parse it
        //two root stages should be present in the XML file
        //1. process model stage
        //2. artifact lifecycle stage
        if (nextStage) {
            //create STAGE instance
            var stage = nextStage;
            var stageId = stage['$'].id;
            ENGINES.get(engineid).Stage_array[stageId] = Object.create(STAGE);
            ENGINES.get(engineid).Stage_array[stageId].init(engineid, stageId, parent, rank);
            // create DATA instance of type DataFlowGuard, connected to STAGE
            for (var dfg in stage['ca:DataFlowGuard']) {
                var guard = stage['ca:DataFlowGuard'][dfg];
                var guardId = guard['$'].id;
                //convert expression
                var sentry = this.convertExpressionToSentry(engineid, guard['$'].expression, guardId);
                //create DATA instance and initialize it
                ENGINES.get(engineid).Data_array[guardId] = Object.create(DATA);
                ENGINES.get(engineid).Data_array[guardId].init(engineid, guardId, stageId, sentry, 'D');
                ENGINES.get(engineid).Stage_array[stageId]._dataGuards.push(guardId);
            }
            // create DATA instance of type ProcessFlowGuard, connected to STAGE
            for (var pfg in stage['ca:ProcessFlowGuard']) {
                var guard = stage['ca:ProcessFlowGuard'][pfg];
                var guardId = guard['$'].id;
                //convert expression
                var sentry = this.convertExpressionToSentry(engineid, guard['$'].expression, guardId);
                //create DATA instance and initialize it
                ENGINES.get(engineid).Data_array[guardId] = Object.create(DATA);
                ENGINES.get(engineid).Data_array[guardId].init(engineid, guardId, stageId, sentry, 'P');
                ENGINES.get(engineid).Stage_array[stageId]._processGuards.push(guardId);
            }
            // create DATA instance of type FaultLogger, connected to STAGE
            for (var fl in stage['ca:FaultLogger']) {
                var guard = stage['ca:FaultLogger'][fl];
                var guardId = guard['$'].id;
                //convert expression
                var sentry = this.convertExpressionToSentry(engineid, guard['$'].expression, guardId);
                //create DATA instance and initialize it
                ENGINES.get(engineid).Data_array[guardId] = Object.create(DATA);
                ENGINES.get(engineid).Data_array[guardId].init(engineid, guardId, stageId, sentry, 'F');
                ENGINES.get(engineid).Stage_array[stageId]._faults.push(guardId);
            }
            // create DATA instance of type Milestone, connected to STAGE
            for (var m in stage['ca:Milestone']) {
                var milestone = stage['ca:Milestone'][m];
                var milestoneId = milestone['$'].id;
                //convert expression
                var sentry = this.convertExpressionToSentry(engineid, milestone['ca:Condition'][0]['$'].expression, milestoneId);
                //create DATA instance and initialize it
                ENGINES.get(engineid).Data_array[milestoneId] = Object.create(DATA);
                ENGINES.get(engineid).Data_array[milestoneId].init(engineid, milestoneId, stageId, sentry, 'M');
                ENGINES.get(engineid).Stage_array[stageId]._milestones.push(milestoneId);
            }

            //define listener for STAGE event
            ENGINES.get(engineid).eventManager.on('STAGE', stageId, function (stage_dep) {
                //dependency array is passed as a stage parameter, when it fires the event
                //in this way, it is possible to determine which elements (STAGE or DATA) munst be updated 
                //TODO: review when update() is called (first DATA then STAGE?)
                if (stage_dep != undefined) {
                    for (var k = 0; k < stage_dep.length; k++) {
                        if (ENGINES.get(engineid).Data_array[stage_dep[k]] != undefined) {
                            //update DATA elements
                            ENGINES.get(engineid).Data_array[stage_dep[k]].update(false);
                        }
                    }

                    for (var k = 0; k < stage_dep.length; k++) {
                        if (ENGINES.get(engineid).Stage_array[stage_dep[k]] != undefined) {
                            //update STAGE elements
                            ENGINES.get(engineid).Stage_array[stage_dep[k]].update();
                        }
                    }
                }
            });

            //handle SubStage elements in XML file
            var subStages = stage['ca:SubStage'];
            if (subStages != undefined && subStages[0] != undefined) {
                //iterate over all child stages, which are linked to the parent stage
                for (var key in subStages) {
                    if (typeof subStages[key] == "object") {
                        //create parent-child link
                        var subStage = subStages[key];
                        var subStageId = subStage['$'].id;
                        ENGINES.get(engineid).Stage_array[stageId]._childs.push(subStageId);
                        //invoke recursive function to create STAGE instance for child stage
                        this.stageParsingRecursive(engineid, subStage, rank + 1, stageId);
                    }
                }
            }
        }
        return;
    },

    eventParsing: function (engineid, event) {
        //create EVENT instance and initialize it
        ENGINES.get(engineid).Event_array[event['$'].id] = Object.create(EVENT);
        ENGINES.get(engineid).Event_array[event['$'].id].init(engineid, event['$'].name);
        //define listener for that event
        ENGINES.get(engineid).eventManager.on('EVENT', event['$'].id, function (event_dep) {
            //when an event is fired, the array with all elements that depend on that event are passed (e.g., PAC isEventOccurring)
            //the listener will then call the update() method on the DATA instance
            if (event_dep != undefined) {
                for (var k = 0; k < event_dep.length; k++) {
                    //check DATA dependencies and call update() method (e.g., updated DFG1 or M1)
                    if (ENGINES.get(engineid).Data_array[event_dep[k]] != undefined) {
                        ENGINES.get(engineid).Data_array[event_dep[k]].update(false);
                    }
                }
            }
        });
    },

    //parse information model
    infoParsing: function (engineid, info) {
        //create INFORMATION instance and initialize it
        var infoId = info['$'].name;
        var pub = info['$'].pub;
        var sub = info['$'].sub;
        ENGINES.get(engineid).Info_array[infoId] = Object.create(INFORMATION);
        ENGINES.get(engineid).Info_array[infoId].init(engineid, infoId, pub, sub);
        //iterate over attributes in XSD section 'xs:attribute'
        var attributes = info['xs:complexType'][0]['xs:attribute'];
        for (var att in attributes) {
            // create ATTRIBUTE instance, initialize it and link it to INFORMATION
            var attributeName = attributes[att]['$'].name;
            var attributeType = attributes[att]['$'].type;
            var attributeUse = attributes[att]['$'].use;
            //TODO check for duplicate attributes
            //Info_array[infoId]._attributes.push(attributeName);
            ENGINES.get(engineid).Info_array[infoId]._attributes[attributeName] = Object.create(ATTRIBUTES);
            ENGINES.get(engineid).Info_array[infoId]._attributes[attributeName].init(engineid, attributeName, infoId, attributeType, attributeUse);
        }
        //define event listener
        ENGINES.get(engineid).eventManager.on('INFO', infoId, function (id, attributes) {
            //when an attribute in INFORMATION is changed, the INFORMATION instance id is passed
            //together with an array containing all changed elements
            //the listener then calls method changeAttributes(attributes)
            if (attributes != undefined) {
                if (ENGINES.get(engineid).Info_array[id] != undefined) {
                    ENGINES.get(engineid).Info_array[id].changeAttributes(attributes);
                }
            }
        });
    },

    setDataListeners: function (engineid) {
        //Add event
        for (var key in ENGINES.get(engineid).Data_array) {
            var data_item = ENGINES.get(engineid).Data_array[key];

            ENGINES.get(engineid).eventManager.on('DATA', data_item.name, function (dep) {
                if (dep != undefined) {
                    for (var k = 0; k < dep.length; k++) {
                        if (ENGINES.get(engineid).Stage_array[dep[k]] != undefined) { ENGINES.get(engineid).Stage_array[dep[k]].update(); }
                    }
                    for (var k = 0; k < dep.length; k++) {
                        if (ENGINES.get(engineid).Data_array[dep[k]] != undefined && ENGINES.get(engineid).Data_array[dep[k]].type == 'P') { ENGINES.get(engineid).Data_array[dep[k]].update(false); }
                    }
                    for (var k = 0; k < dep.length; k++) {
                        if (ENGINES.get(engineid).Data_array[dep[k]] != undefined && ENGINES.get(engineid).Data_array[dep[k]].type == 'F') { ENGINES.get(engineid).Data_array[dep[k]].update(false); }
                    }
                    for (var k = 0; k < dep.length; k++) {
                        if (ENGINES.get(engineid).Data_array[dep[k]] != undefined && ENGINES.get(engineid).Data_array[dep[k]].type == 'D') { ENGINES.get(engineid).Data_array[dep[k]].update(false); }
                    }
                    for (var k = 0; k < dep.length; k++) {
                        if (ENGINES.get(engineid).Data_array[dep[k]] != undefined && ENGINES.get(engineid).Data_array[dep[k]].type == 'M') { ENGINES.get(engineid).Data_array[dep[k]].update(false); }
                    }
                }
            });
        }
    },

    setDependencies: function (engineid) {
        for (key in ENGINES.get(engineid).Dependency_Array) {
            for (dep in ENGINES.get(engineid).Dependency_Array[key]) {
                if (ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Dependency_Array[key][dep]] != undefined) {
                    //Releated guard
                    if (ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.indexOf(ENGINES.get(engineid).Dependency_Array[key]) == -1)
                        ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.push(key);
                }
                else if (ENGINES.get(engineid).Event_array[ENGINES.get(engineid).Dependency_Array[key][dep]] != undefined) {
                    //Releated guard
                    if (ENGINES.get(engineid).Event_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.indexOf(ENGINES.get(engineid).Dependency_Array[key]) == -1)
                        ENGINES.get(engineid).Event_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.push(key);
                }
                else if (ENGINES.get(engineid).Stage_array[ENGINES.get(engineid).Dependency_Array[key][dep]] != undefined) {
                    //Releated guard
                    if (ENGINES.get(engineid).Stage_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.indexOf(ENGINES.get(engineid).Dependency_Array[key]) == -1)
                        ENGINES.get(engineid).Stage_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.push(key);
                }
                else if (ENGINES.get(engineid).Info_array[ENGINES.get(engineid).Dependency_Array[key][dep]] != undefined) {
                    //Releated guard
                    if (ENGINES.get(engineid).Info_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.indexOf(ENGINES.get(engineid).Dependency_Array[key]) == -1)
                        ENGINES.get(engineid).Info_array[ENGINES.get(engineid).Dependency_Array[key][dep]]._array_dep.push(key);
                }
            }
        }
    },
}


function startEngine(engineid, xsdInfoModel, xmlProcessModel) {
    var parseString = xml2js.parseString;
    parseString(xmlProcessModel, function (err, result) {
        var processModel = result;
        parseString(xsdInfoModel, function (err, result) {
            var infoModel = result;
            // initialize and start engine
            ENGINES.get(engineid).initModel(processModel, infoModel);
        });
    });
}

module.exports = {
    /**
     * Has free Engine slot in the module
     * @returns Returns true if yes, false otherwise
     */
    hasFreeSlot: function () {
        if (ENGINES.size < MAX_ENGINES) {
            return true
        }
        return false
    },

    /**
     * Gets the number of how many Engines can the module accomodate
     * @returns Number of max engines
     */
    getCapacity: function () {
        return MAX_ENGINES
    },

    /**
     * 
     * @returns Current number of Engines in the module
     */
    getEngineNumber: function () {
        return ENGINES.size
    },

    /**
     * Updates the lifecycle status (RUNNING/FINISHED) of an engine instance
     * @param {string} engineid 
     * @param {string} newstatus 
     */
    setEngineLifeCycleStatus(engineid, newstatus) {
        if (ENGINES.has(engineid)) {
            ENGINES.get(engineid).lifeCycleStatus = newstatus
        }
    },

    /**
     * Get details of one Engine instance
     * @param {*} engineid Engine instance ID
     * @returns Engine Details object. If the Engine with the provided ID is not exists then it will return with an empty object
     */
    getEngineDetails: function (engineid) {
        if (ENGINES.has(engineid)) {
            return {
                name: engineid,
                type: ENGINES.get(engineid).process_type,
                instance_id: ENGINES.get(engineid).process_instance,
                perspective: ENGINES.get(engineid).process_perspective,
                uptime: (new Date().getTime() / 1000 / 60 - ENGINES.get(engineid).startTime).toPrecision(2).toString() + " min",
                status: ENGINES.get(engineid).lifeCycleStatus
            }
        }
        return {}
    },

    /**
     * Get the list of Engines in the module
     * @returns List of Engines with their important details
     */
    getEngineList: function () {
        var result = []

        for (let [key, value] of ENGINES) {
            result.push(this.getEngineDetails(key))
        }
        //Sorting the engines based on their name and adding index
        result.sort((a, b) => {
            const nameA = a.name.toUpperCase(); // ignore upper and lowercase
            const nameB = b.name.toUpperCase(); // ignore upper and lowercase
            if (nameA < nameB) {
                return -1;
            }
            if (nameA > nameB) {
                return 1;
            }

            // names must be equal
            return 0;
        });
        var cnt = 1
        result.forEach(element => {
            element['index'] = cnt
            cnt++
        });
        return result
    },

    /**
     * Get the engines which meet with provided rules
     * @param {Object} rules Object containing rules
     * @returns List of Engine ID-s
     */
    getFilteredProcesses: function (rules) {
        var filteredEngines = new Set()
        for (let [key, value] of ENGINES) {
            if (Validator.isRulesSatisfied(value, rules)) {
                filteredEngines.add(key)
            }
        }
        var result = []
        filteredEngines.forEach(engine => {
            result.push(this.getEngineDetails(engine))
        });
        return result
    },

    /**
     * 
     * @param {String} engineid 
     * @returns Returns True of the engine with the provided ID exists, false otherwise
     */
    exists: function (engineid) {
        return ENGINES.has(engineid)
    },

    /**
     * Returns the engines which are part of the process specified by its ID
     * @param {String} process_instance_id 
     * @returns An array of engines (and their details). If no Engine instance is the part of the Process then it returns and empty array
     */
    getEnginesOfProcess(process_instance_id) {
        var result = []
        for (let [key, value] of ENGINES) {
            if (ENGINES.get(key).process_instance == process_instance_id) {
                result.push(this.getEngineDetails(key))
            }
        }
        return result
    },

    /**
     * Sets the event router the module use
     * @param {*} publishfunction Reference to the publisher function of the Event Router
     */
    setEventRouter: function (publishfunction) {
        SUBSCRIBE = publishfunction
    },

    /**
     * Creates a new Engine based on the provided attributes
     * @param {*} engineid 
     * @param {*} informalModel 
     * @param {*} processModel 
     * @returns 'created' if the operation was successfull, "already_exists" if the ID is already used
     */
    createNewEngine: async function (engineid, informalModel, processModel, stakeholders) {
        var start = Date.now()
        if (ENGINES.has(engineid)) {
            return "already_exists"
        }
        STAGE_EVENT_ID.set(engineid, 0)
        ENGINES.set(engineid, new Engine(engineid, stakeholders))
        DDB.writeNewProcessInstance(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_instance + '__' + ENGINES.get(engineid).process_perspective, stakeholders, Date.now() / 1000, CONNCONFIG.getConfig().primary_broker.host, CONNCONFIG.getConfig().primary_broker.port.toString())
        startEngine(engineid, informalModel, processModel)

        const millis = Date.now() - start;
        EVENTR.publishProcessLifecycleEvent('created', engineid, ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_instance, stakeholders)
        return 'created'
    },

    /**
     * Terminates and removes and engine specified by its ID
     * @param {String} engineid Engine to terminate
     * @returns 
     */
    removeEngine: async function (engineid) {
        if (!ENGINES.has(engineid)) {
            return "not_defined"
        }
        //Engine found
        //Perform database operations to update aggregation statistics
        await DDB.increaseProcessTypeInstanceCounter(ENGINES.get(engineid).process_type)

        var metrics = []
        for (var key in ENGINES.get(engineid).Stage_array) {
            metrics.push({
                name: key,
                state: ENGINES.get(engineid).Stage_array[key].state,
                status: ENGINES.get(engineid).Stage_array[key].status,
                compliance: ENGINES.get(engineid).Stage_array[key].compliance
            })
            //await DDB.increaseProcessTypeStatisticsCounter(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_perspective, key, ENGINES.get(engineid).Stage_array[key].state)
            //await DDB.increaseProcessTypeStatisticsCounter(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_perspective, key, ENGINES.get(engineid).Stage_array[key].status)
            //await DDB.increaseProcessTypeStatisticsCounter(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_perspective, key, ENGINES.get(engineid).Stage_array[key].compliance)
        }
        await DDB.increaseProcessTypeStatisticsCounter(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_perspective, metrics)
        var outcome = 'SUCCESS'
        for (var key in ENGINES.get(engineid).Stage_array) {
            if (ENGINES.get(engineid).Stage_array[key].state == 'faulty' || ENGINES.get(engineid).Stage_array[key].compliance == 'outOfOrder') {
                outcome = 'FAULTY'
                break
            }
        }
        await DDB.closeOngoingProcessInstance(ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_instance + '__' + ENGINES.get(engineid).process_perspective, Date.now() / 1000, outcome)
        STAGE_EVENT_ID.delete(engineid)
        EVENTR.publishProcessLifecycleEvent('destructed', engineid, ENGINES.get(engineid).process_type, ENGINES.get(engineid).process_instance, ENGINES.get(engineid).stakeholders)
        ENGINES.delete(engineid)
        return 'deleted'
    },

    /**
     * Resets the specified engine
     * @param {String} engineid Engine to reset
     * @returns 
     */
    resetEngine: function (engineid) {
        if (!ENGINES.has(engineid)) {
            return "not_defined"
        }
        ENGINES.get(engineid).reset()
        return 'resetted'
    },

    /**
     * Notify engine about Info Model change. Used by Event Router
     * @param {*} engineid 
     * @param {*} name Name of Info Model Entry
     * @param {*} value New value of the changed parameter 
     * @returns 
     */
    updateInfoModel: function (engineid, name, value) {
        if (!ENGINES.has(engineid)) {
            return "not_defined"
        }
        return ENGINES.get(engineid).updateInfoModel(name, value)
    },

    // FRONT-END SUPPORT FUNCTIONS BEGIN
    // create JSON to represent model in UI
    getCompleteDiagram: function (engineid) {
        //build json model for GUI
        var json_model = [];
        if (!ENGINES.has(engineid)) {
            return json_model
        }
        for (var key in ENGINES.get(engineid).Stage_array) {
            if (ENGINES.get(engineid).Stage_array[key].rank == 0)
                json_model.push(ENGINES.get(engineid).UI.completeDiagram({}, ENGINES.get(engineid).Stage_array[key], 0));
        }
        return json_model;
    },

    // create JSON to represent model in UI
    getCompleteNodeDiagram: function (engineid) {
        //build json model for GUI
        var json_model = [];
        if (!ENGINES.has(engineid)) {
            return json_model
        }
        for (var key in ENGINES.get(engineid).Stage_array) {
            var stageM = {};
            stageM.name = ENGINES.get(engineid).Stage_array[key].name;
            stageM.key = ENGINES.get(engineid).Stage_array[key].name;
            if (ENGINES.get(engineid).Stage_array[key].state == 'unopened' && ENGINES.get(engineid).Stage_array[key].compliance == 'onTime') {
                stageM.color = "silver";
            }
            else if (ENGINES.get(engineid).Stage_array[key].compliance == 'skipped') {
                stageM.color = "gray";
            }
            else if (ENGINES.get(engineid).Stage_array[key].state == 'opened' && ENGINES.get(engineid).Stage_array[key].compliance == 'onTime') {
                stageM.color = "orange";
            }
            else if (ENGINES.get(engineid).Stage_array[key].state == 'closed' && ENGINES.get(engineid).Stage_array[key].compliance == 'onTime') {
                stageM.color = "darkgreen";
            }
            else if (ENGINES.get(engineid).Stage_array[key].compliance == 'outOfOrder') {
                stageM.color = "red";
            }

            stageM.isGroup = true;
            stageM.group = ENGINES.get(engineid).Stage_array[key].parent;
            stageM.inservices = [];
            for (var key2 in ENGINES.get(engineid).Stage_array[key]._dataGuards) {
                var guard = {};
                guard.name = ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Stage_array[key]._dataGuards[key2]].name
                stageM.inservices.push(guard);
            }
            json_model.push(stageM);

            for (var key2 in ENGINES.get(engineid).Stage_array[key]._dataGuards) {
                var guard = {};
                guard.name = 'DFG' + key2;
                guard.key = ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Stage_array[key]._dataGuards[key2]].name;
                guard.color = "silver"
                guard.group = ENGINES.get(engineid).Data_array[ENGINES.get(engineid).Stage_array[key]._dataGuards[key2]].stage;
                json_model.push(guard);
            }
        }
        return json_model;
    },

    getInfoModel: function (engineid) {
        var json_model = [];
        if (!ENGINES.has(engineid)) {
            return json_model
        }
        for (var key in ENGINES.get(engineid).Info_array) {
            var infoM = {};
            infoM.name = ENGINES.get(engineid).Info_array[key].name;
            infoM.attributes = [];
            for (var key2 in ENGINES.get(engineid).Info_array[key]._attributes) {
                if (ENGINES.get(engineid).Info_array[key]._attributes[key2].name != 'timestamp')
                    infoM.attributes.push(ENGINES.get(engineid).Info_array[key]._attributes[key2]);
            }
            json_model.push(infoM);
        }
        return json_model;
    },

    getEventModel: function (engineid) {
        var json_model = [];
        if (!ENGINES.has(engineid)) {
            return json_model
        }
        for (var key in ENGINES.get(engineid).Event_array) {
            var extM = {};
            extM.name = ENGINES.get(engineid).Event_array[key].name;
            extM.value = ENGINES.get(engineid).Event_array[key].value;
            json_model.push(extM);
        }
        return json_model;
    },
    // FRONT-END SUPPORT FUNCTIONS END

    //DEPRECHATED FUNCTIONS BEGIN, KEPT FOR BACKWARD COMPATIBILITY
    getDataArray: function (engineid) {
        return ""//ENGINES.get(engineid).Data_array
    },
    getStageArray: function (engineid) {
        return ""//ENGINES.get(engineid).Stage_array
    },

    getDebugLog: function (engineid) {
        //TODO
        return ''
    },
    //DEPRECHATED FUNCTIONS END, KEPT FOR BACKWARD COMPATIBILITY
}
/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var util = require('util');
var async = require('async');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
var OPERATION_SCHEMAS = {
    "execute": {
        "type": "object",
        "properties": {
            "userUuid": {"type": "string"},
            "deviceUuid": {"type": "string"},
            "cmd": {
                "type": "object",
                "properties": {
                    "cmdCode": {"type": "string"},
                    "cmdName": {"type": "string"},
                    "parameters": {
                        "type": ["string", "number", "object","array"]
                    }
                },
                "required": ["cmdCode", "cmdName"]
            }

        },
        "required": ["deviceUuid", "cmd"]
    }
};
var regExp = /\[.*\]/;
var regExpAry = /\[\?.*\]/;
var regExpSig = /\[\$.*\]/;
var TAG_REQUEST = "$Request";
var TAG_RESPONSE = "$Response";

var getDeviceTypeFromZkPath = function (zkPath) {
    var pathNodes = zkPath.split("/");
    if (pathNodes.length >= 6) {
        return pathNodes[3] + pathNodes[4] + pathNodes[5] + pathNodes[6];
    }
    else {
        return null;
    }
};

var getDeviceCmdCode = function (zkPath) {
    var pathNodes = zkPath.split("/");
    if (pathNodes.length >= 8) {
        return pathNodes[8];
    }
    else {
        return null;
    }
};

var getDeviceCmdProperty = function (zkPath) {
    var pathNodes = zkPath.split("/");
    if (pathNodes.length >= 9) {
        return pathNodes[9];
    }
    else {
        return null;
    }
};

var getZkNodeChildren = function (zkClient, zkPath, watch, callback) {
    if (util.isNullOrUndefined(callback)) {
        callback = watch;
        watch = null;
    }
    zkClient.getChildren(zkPath,
        watch,
        function (error, children, stat) {
            if (error) {
                callback({
                    errorId: 208001,
                    errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
                });
            }
            else {
                callback(null, zkPath, children);
            }
        });
};

var getZkNodeData = function (zkClient, zkPath, watch, callback) {
    if (util.isNullOrUndefined(callback)) {
        callback = watch;
        watch = null;
    }
    zkClient.getData(zkPath,
        watch,
        function (error, data, stat) {
            if (error) {
                callback({
                    errorId: 208001,
                    errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
                });
            }
            else {
                var dataStr = data.toString('utf8');
                callback(null, zkPath, dataStr);
            }
        });
};

var parseConf = function (confObj, request, response) {
    var retObj = JSON.parse(JSON.stringify(confObj));
    if (util.isArray(confObj)) {
        for (var k = 0; k < confObj.length; ++k) {
            if (typeof (confObj[k]) === "object") {
                confObj[k] = parseConf(confObj[k], request, response);
            }
        }
    }
    else {
        for (var item in confObj) {
            var itemObj = confObj[item];
            if (typeof (itemObj) === "string") {
                if (itemObj.substr(0, 8) === TAG_REQUEST || itemObj.substr(0, 9) === TAG_RESPONSE) {
                    if (!util.isNullOrUndefined(regExp.exec(item))) {
                        delete retObj[item];
                        if (!util.isNullOrUndefined(regExpAry.exec(item))) {
                            var itemKey = regExpAry.exec(item);
                            if (util.isNullOrUndefined(itemKey) && !util.isArray(itemKey)) {
                                continue;
                            }
                            item = item.substr(0, itemKey["index"]);
                            itemKey = itemKey[0];
                            itemKey = itemKey.substring(3, itemKey.length - 1);
                            if (typeof (itemObj) === "string") {
                                var itemValue = regExpAry.exec(itemObj);
                                if (util.isNullOrUndefined(itemValue) && !util.isArray(itemValue)) {
                                    continue;
                                }
                                itemObj = itemObj.substr(0, itemValue["index"]);
                                itemValue = itemValue[0];
                                itemValue = itemValue.substring(3, itemValue.length - 1);
                                if (itemObj.substr(0, 8) === TAG_REQUEST || itemObj.substr(0, 9) === TAG_RESPONSE) {
                                    var valueCpy = JSON.parse(JSON.stringify(itemObj.substr(0, 8) === TAG_REQUEST ? request : response));
                                    if (valueCpy) {
                                        var pathNodes2 = itemObj.split(".");
                                        for (var m = 1; m < pathNodes2.length && !util.isNullOrUndefined(valueCpy); ++m) {
                                            var tempPathNode = pathNodes2[m];
                                            if (!regExpAry.test(tempPathNode)) {
                                                valueCpy = valueCpy[tempPathNode];
                                            }
                                        }
                                    }
                                    if (util.isArray(valueCpy)) {
                                        for (var n = 0; n < valueCpy.length; ++n) {
                                            var itemKeyPathNodes = itemKey.split(".");
                                            var itemNameValue = JSON.parse(JSON.stringify(valueCpy[n]));
                                            for (var x = 0; x < itemKeyPathNodes.length; ++x) {
                                                itemNameValue = itemNameValue[itemKeyPathNodes[x]];
                                            }
                                            var temItemKey = item + "." + itemNameValue;
                                            var itemValuePathNodes = itemValue.split(".");
                                            var itemValueValue = JSON.parse(JSON.stringify(valueCpy[n]));
                                            for (var y = 0; y < itemValuePathNodes.length; ++y) {
                                                itemValueValue = itemValueValue[itemValuePathNodes[y]];
                                            }
                                            retObj[temItemKey] = itemValueValue;
                                        }
                                    }
                                }
                            }
                        }
                        else if (!util.isNullOrUndefined(regExpSig.exec(item))) {
                            var itemKey = regExpSig.exec(item);
                            if (util.isNullOrUndefined(itemKey) && !util.isArray(itemKey)) {
                                continue;
                            }
                            item = item.substr(0, itemKey["index"]);
                            itemKey = itemKey[0];
                            itemKey = itemKey.substring(1, itemKey.length - 1);
                            var itemNameTmp = JSON.parse(JSON.stringify(itemKey.substr(0, 8) === TAG_REQUEST ? request : response));
                            if (itemNameTmp) {
                                var itemNamePathNodes = itemKey.split(".");
                                for (var p = 1; p < itemNamePathNodes.length && !util.isNullOrUndefined(itemNameTmp); ++p) {
                                    itemNameTmp = itemNameTmp[itemNamePathNodes[p]];
                                }
                            }
                            var itemValueTmp = JSON.parse(JSON.stringify(itemObj.substr(0, 8) === TAG_REQUEST ? request : response));
                            if (itemValueTmp) {
                                var itemValuePathNodes = itemObj.split(".");
                                for (var r = 1; r < itemValuePathNodes.length && !util.isNullOrUndefined(itemValueTmp); ++r) {
                                    itemValueTmp = itemValueTmp[itemValuePathNodes[r]];
                                }
                            }
                            var itemName = item + "." + itemNameTmp;
                            retObj[itemName] = itemValueTmp;
                        }
                    }
                    else {
                        var valueTmp = JSON.parse(JSON.stringify(itemObj.substr(0, 8) === TAG_REQUEST ? request : response));
                        if (valueTmp) {
                            var pathNodes1 = itemObj.split(".");
                            for (var l = 1; l < pathNodes1.length && !util.isNullOrUndefined(valueTmp); ++l) {
                                valueTmp = valueTmp[pathNodes1[l]];
                            }
                        }
                        retObj[item] = valueTmp;
                    }
                }
            }
            else if (item.substr(0, 8) === TAG_REQUEST || item.substr(0, 9) === TAG_RESPONSE) {
                var value = item.substr(0, 8) === TAG_REQUEST ? request : response;
                if (value) {
                    var pathNodes = item.split(".");
                    for (var i = 1; i < pathNodes.length && !util.isNullOrUndefined(value); ++i) {
                        value = value[pathNodes[i]];
                    }
                    if (util.isArray(itemObj)) {
                        retObj = undefined;
                        for (var j = 0; j < itemObj.length; ++j) {
                            if (itemObj[j].key === value) {
                                retObj = itemObj[j].value;
                                break;
                            }
                            else if (itemObj[j].key === "^default") {
                                retObj = itemObj[j].value;
                            }
                        }
                        return retObj;
                    }
                }
            }
            else {
                if (typeof (itemObj) === "object") {
                    retObj[item] = parseConf(itemObj, request, response);
                }
            }
        }
        return retObj;
    }
};

function Executor(conx, uuid, token, configurator) {
    this.devicesConf = {};
    VirtualDevice.call(this, conx, uuid, token, configurator);
}
util.inherits(Executor, VirtualDevice);

/**
 * 远程RPC回调函数
 * @callback onMessage~execute
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{string},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 执行命令
 * @param {object} message:输入消息
 * @param {onMessage~execute} peerCallback: 远程RPC回调
 * */
Executor.prototype.execute = function (message, peerCallback) {
    var self = this;
    logger.warn(message);
    var responseMessage = {retCode: 200, description: "Success.", data: {}};
    self.messageValidate(message, OPERATION_SCHEMAS.execute, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            async.waterfall([
                /*get device info*/
                function (innerCallback) {
                    var msg = {
                        devices: self.configurator.getConfRandom("services.device_manager"),
                        payload: {
                            cmdName: "getDevice",
                            cmdCode: "0003",
                            parameters: {
                                uuid: message.deviceUuid
                            }
                        }
                    };
                    if(!util.isNullOrUndefined(message.userUuid)){
                        msg.payload.parameters.userId = message.userUuid;
                    }
                    self.message(msg, function (response) {
                        if (response.retCode === 200) {
                            var deviceInfo = response.data;
                            if (util.isArray(response.data)) {
                                deviceInfo = response.data[0];
                            }
                            if (util.isNullOrUndefined(deviceInfo)) {
                                innerCallback({errorId: response.retCode, errorMsg: "Can not find the device."});
                            }
                            else {
                                innerCallback(null, deviceInfo);
                            }

                        } else {
                            innerCallback({errorId: response.retCode, errorMsg: response.description});
                        }
                    });
                },
                function (deviceInfo, innerCallback) {
                    var deviceType = deviceInfo.type.id;
                    var cmdId = message.cmd.cmdCode;
                    var deviceConf = self.devicesConf[deviceType];
                    if (!util.isNullOrUndefined(deviceConf)
                        && !util.isNullOrUndefined(deviceConf[cmdId])
                        && !util.isNullOrUndefined(deviceConf[cmdId]["parameters"])
                    ) {
                        try {
                            var parametersConf = JSON.parse(deviceConf[cmdId]["parameters"]);
                            var schema = parametersConf["properties"]["request"];
                            self.messageValidate(message.cmd.parameters, schema, function (error) {
                                if (error) {
                                    innerCallback({errorId: error.retCode, errorMsg: error.description});
                                }
                                else {
                                    innerCallback(null, deviceInfo);
                                }
                            });
                        }
                        catch (e) {
                            logger.error(207000, e);
                            innerCallback(null, deviceInfo);
                        }
                    }
                    else {
                        innerCallback(null, deviceInfo);
                    }
                },
                function (deviceInfo, innerCallback) {
                    var msg = {};
                    var deviceType = deviceInfo.type.id;
                    var cmdId = message.cmd.cmdCode;
                    var deviceConf = self.devicesConf[deviceType];
                    if (!util.isNullOrUndefined(deviceConf)
                        && !util.isNullOrUndefined(deviceConf[cmdId])
                    ) {
                        var protocol = deviceConf[cmdId]["protocol"];
                        if (protocol === "TCP") {
                            if (!util.isNullOrUndefined(deviceInfo.extra.connection)
                                && !util.isNullOrUndefined(deviceInfo.extra.connection.socket)) {
                                msg = {
                                    devices: self.configurator.getConfRandom("services.session"),
                                    payload: {
                                        cmdName: "sendMessage",
                                        cmdCode: "0001",
                                        parameters: {
                                            sendTo: deviceInfo.extra.connection.socket,
                                            data: JSON.stringify(message.cmd.parameters)
                                        }
                                    }
                                };
                            }
                            else {
                                innerCallback({errorId: 207001, errorMsg: "TCP device connection loosed."});
                                return;
                            }
                        }
                        else if(protocol === "MQTT"){
                            msg = {
                                devices: [deviceInfo.uuid],
                                payload: message.cmd
                            };
                            if (deviceInfo.type.id.substr(0, 4) === "040B") {
                                msg = {
                                    devices: [deviceInfo.owner],
                                    payload: {
                                        cmdName: "forward",
                                        cmdCode: "0001",
                                        parameters: {
                                            uuid: deviceInfo.uuid,
                                            deviceType: deviceInfo.type.id,
                                            cmd: message.cmd
                                        }
                                    }
                                };
                            }
                        }
                        else {
                            innerCallback({errorId: 207002, errorMsg: "Unsupported protocol type:" + protocol});
                            return;
                        }
                    }
                    else{
                        innerCallback({errorId: 207003,
                            errorMsg: "no command config:[name:" + message.cmd.cmdName +",code:"+message.cmd.cmdCode});
                        return;
                    }
                    /*var handleResult = function (response) {
                     if (response.retCode === 200) {
                     var result = response.data;
                     var deviceType = deviceInfo.type.id;
                     var cmdId = message.cmd.cmdCode;
                     var deviceConf = self.devicesConf[deviceType];
                     if (!util.isNullOrUndefined(deviceConf)
                     && !util.isNullOrUndefined(deviceConf[cmdId])
                     && !util.isNullOrUndefined(deviceConf[cmdId]["update"])
                     ) {
                     try {
                     var updateConf = JSON.parse(deviceConf[cmdId]["update"]);
                     updateConf = parseConf(updateConf, message.cmd.parameters, result);
                     updateConf.uuid = deviceInfo.uuid;
                     var msg = {
                     devices: self.configurator.getConfRandom("services.device_manager"),
                     payload: {
                     cmdName: "deviceUpdate",
                     cmdCode: "0004",
                     parameters: updateConf
                     }
                     };
                     self.message(msg, function (response) {
                     if (response.retCode !== 200) {
                     logger.error(response.retCode, response.description);
                     }
                     });
                     }
                     catch (e) {
                     innerCallback({errorId: 207000, errorMsg: e});
                     }
                     }
                     innerCallback(null, result);
                     } else {
                     innerCallback({errorId: response.retCode, errorMsg: response.description});
                     }
                     };*/
                    logger.debug(msg);
                    self.message(msg, function (response) {
                        if (response.retCode === 200) {
                            var result = response.data;
                            var deviceType = deviceInfo.type.id;
                            var cmdId = message.cmd.cmdCode;
                            var deviceConf = self.devicesConf[deviceType];
                            if (!util.isNullOrUndefined(deviceConf)
                                && !util.isNullOrUndefined(deviceConf[cmdId])
                                && !util.isNullOrUndefined(deviceConf[cmdId]["update"])
                            ) {
                                try {
                                    var updateConf = JSON.parse(deviceConf[cmdId]["update"]);
                                    updateConf = parseConf(updateConf, message.cmd.parameters, result);
                                    updateConf.uuid = deviceInfo.uuid;
                                    var msg = {
                                        devices: self.configurator.getConfRandom("services.device_manager"),
                                        payload: {
                                            cmdName: "deviceUpdate",
                                            cmdCode: "0004",
                                            parameters: updateConf
                                        }
                                    };
                                    self.message(msg, function (response) {
                                        if (response.retCode !== 200) {
                                            logger.error(response.retCode, response.description);
                                        }
                                    });
                                }
                                catch (e) {
                                    innerCallback({errorId: 207000, errorMsg: e});
                                }
                            }
                            innerCallback(null, result);
                        } else {
                            innerCallback({errorId: response.retCode, errorMsg: response.description});
                        }
                    });
                }
            ], function (error, result) {
                if (error) {
                    responseMessage.retCode = error.errorId;
                    responseMessage.description = error.errorMsg;
                }
                else {
                    responseMessage.data = result;
                }
                peerCallback(responseMessage);
            });
        }
    });
};

Executor.prototype.init = function () {
    var self = this;
    async.waterfall([
            function (innerCallback) {
                var zkPath = "/devices/types";
                getZkNodeChildren(self.configurator.zkClient, zkPath, function (error, path, children) {
                    if (error) {
                        innerCallback(null, []);
                    }
                    else {
                        for (var i = 0, len = children.length; i < len; ++i) {
                            children[i] = path + "/" + children[i];
                        }
                        innerCallback(null, children);
                    }
                });
            },
            function (parent, innerCallback) { //parent = /devices/types/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx/xx/xxxxxx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i] + "/commands", function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            var deviceType = getDeviceTypeFromZkPath(path);
                            self.devicesConf[deviceType] = {};
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                var cmdId = children[j];
                                self.devicesConf[deviceType][cmdId] = {};
                                childrenPath.push(path + "/" + cmdId);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx/xx/xxxxxx/commands/xxxx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                var property = children[j];
                                childrenPath.push(path + "/" + property);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            }
        ],
        function (error, parent) {//parent = /devices/types/xx/xx/xx/xxxxxx/commands/xxxx/<property>
            if (!error) {
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeData(self.configurator.zkClient
                        , parent[i]
                        , function (error, path, data) {
                            if (!error && data && data !== "") {
                                try {
                                    var deviceType = getDeviceTypeFromZkPath(path);
                                    var cmdCode = getDeviceCmdCode(path);
                                    var property = getDeviceCmdProperty(path);
                                    self.devicesConf[deviceType][cmdCode][property] = data;
                                }
                                catch (e) {
                                    logger.error(207000, e);
                                }
                            }
                        })
                }
            }
        });
};

module.exports = {
    Service: Executor,
    OperationSchemas: OPERATION_SCHEMAS
};
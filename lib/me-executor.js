/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var _ = require('lodash');
var util = require('util');
var async = require('async');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
const OPERATION_SCHEMAS = {
  "execute": {
    "type": "object",
    "properties": {
      "userUuid": {"type": "string"},
      "deviceUuid": {"type": ["string", "array"]},
      "cmd": {
        "type": "object",
        "properties": {
          "cmdCode": {"type": "string"},
          "cmdName": {"type": "string"},
          "parameters": {
            "type": ["string", "number", "object", "array"]
          }
        },
        "required": ["cmdCode", "cmdName"]
      }

    },
    "required": ["deviceUuid", "cmd"]
  }
};
const regExp = /\[.*\]/;
const regExpAry = /\[\?.*\]/;
const regExpSig = /\[\$.*\]/;
const TAG_REQUEST = "$Request";
const TAG_RESPONSE = "$Response";
const OPEN_ALL_CMD = 1;
const CLOSE_ALL_CMD = 0;
const KMJ_LED_TYPE = "05060B052000";
const HL_THERMOSTAT_TYPE = "050608070001";
const SUPPORTED_RECEIVER_TYPES = [
  "040B09050101", "040B09050111", "040B09050201",
  "040B09050102", "040B09050112", "040B09050202",
  "040B09050103", "040B09050113", "040B09050203"
];
const KMJ_LED_SCENE = {
  goodnight: [
    {
      cmdCode: "0001",
      cmdName: "set_turnmode",
      parameters: {
        mode: 1
      }
    },
    {
      cmdCode: "0002",
      cmdName: "set_color",
      parameters: {
        groupx_num: 1,
        groupy_num: 1,
        mode: 0,
        RGBy: [
          {
            RGBx: [
              {
                R: 255,
                G: 195,
                B: 124
              }
            ]
          }
        ]
      }
    },
    {
      cmdCode: "0003",
      cmdName: "set_luminance",
      parameters: {
        min: 50,
        max: 50
      }
    },
    {
      cmdCode: "0004",
      cmdName: "set_period",
      parameters: {
        high_period: 0,
        low_period: 0,
        changex_period: 0,
        changey_period: 0
      }
    }
  ],
  colorful: [
    {
      cmdCode: "0001",
      cmdName: "set_turnmode",
      parameters: {
        mode: 1
      }
    },
    {
      cmdCode: "0002",
      cmdName: "set_color",
      parameters: {
        groupx_num: 4,
        groupy_num: 5,
        mode: 1,
        RGBy: [
          {
            RGBx: [
              {
                R: 255,
                G: 0,
                B: 0
              },
              {
                R: 0,
                G: 0,
                B: 255
              },
              {
                R: 0,
                G: 255,
                B: 0
              },
              {
                R: 128,
                G: 0,
                B: 255
              }
            ]
          },
          {
            RGBx: [
              {
                R: 255,
                G: 128,
                B: 0
              },
              {
                R: 238,
                G: 238,
                B: 0
              },
              {
                R: 255,
                G: 20,
                B: 147
              },
              {
                R: 0,
                G: 255,
                B: 0
              }
            ]
          },
          {
            RGBx: [
              {
                R: 255,
                G: 255,
                B: 0
              },
              {
                R: 128,
                G: 0,
                B: 255
              },
              {
                R: 238,
                G: 238,
                B: 0
              },
              {
                R: 0,
                G: 255,
                B: 0
              }
            ]
          },
          {
            RGBx: [
              {
                R: 128,
                G: 0,
                B: 255
              },
              {
                R: 0,
                G: 255,
                B: 255
              },
              {
                R: 0,
                G: 255,
                B: 0
              },
              {
                R: 0,
                G: 0,
                B: 255
              }
            ]
          },
          {
            RGBx: [
              {
                R: 78,
                G: 238,
                B: 148
              },
              {
                R: 255,
                G: 20,
                B: 147
              },
              {
                R: 0,
                G: 255,
                B: 0
              },
              {
                R: 255,
                G: 64,
                B: 64
              }
            ]
          }
        ]
      }
    },
    {
      cmdCode: "0003",
      cmdName: "set_luminance",
      parameters: {
        min: 100,
        max: 100
      }
    },
    {
      cmdCode: "0004",
      cmdName: "set_period",
      parameters: {
        high_period: 0,
        low_period: 0,
        changex_period: 0,
        changey_period: 2
      }
    }
  ],
  gorgeous: [
    {
      cmdCode: "0001",
      cmdName: "set_turnmode",
      parameters: {
        mode: 2
      }
    },
    {
      cmdCode: "0002",
      cmdName: "set_color",
      parameters: {
        groupx_num: 10,
        groupy_num: 1,
        mode: 0,
        RGBy: [
          {
            RGBx: [
              {
                R: 0,
                G: 245,
                B: 255
              },
              {
                R: 84,
                G: 255,
                B: 159
              },
              {
                R: 0,
                G: 205,
                B: 0
              },
              {
                R: 255,
                G: 255,
                B: 0
              },
              {
                R: 255,
                G: 106,
                B: 106
              },
              {
                R: 255,
                G: 64,
                B: 64
              },
              {
                R: 255,
                G: 0,
                B: 0
              },
              {
                R: 255,
                G: 20,
                B: 147
              },
              {
                R: 0,
                G: 0,
                B: 255
              },
              {
                R: 72,
                G: 209,
                B: 204
              }
            ]
          }
        ]
      }
    },
    {
      cmdCode: "0003",
      cmdName: "set_luminance",
      parameters: {
        min: 0,
        max: 100
      }
    },
    {
      cmdCode: "0004",
      cmdName: "set_period",
      parameters: {
        high_period: 20,
        low_period: 20,
        changex_period: 0,
        changey_period: 40
      }
    }
  ],
  breathing: [
    {
      cmdCode: "0001",
      cmdName: "set_turnmode",
      parameters: {
        mode: 1
      }
    },
    {
      cmdCode: "0003",
      cmdName: "set_luminance",
      parameters: {
        min: 0,
        max: 100
      }
    },
    {
      cmdCode: "0004",
      cmdName: "set_period",
      parameters: {
        high_period: 20,
        low_period: 20,
        changex_period: 0,
        changey_period: 0
      }
    }
  ]
};
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

var receiverCmdCompleting = function (deviceInfo, cmdData) {
  var deviceType = deviceInfo.type.id;
  var status = deviceInfo.extra.items.status;
  var switchBits = 0;
  var chanelBits = 0;
  if ("040B09050101" === deviceType || "040B09050111" === deviceType || "040B09050201" === deviceType) {
    if (OPEN_ALL_CMD === cmdData) {
      cmdData = 257;
    }
    else if (CLOSE_ALL_CMD === cmdData) {
      cmdData = 256;
    }
    else if (257 !== cmdData && 256 !== cmdData) {
      switchBits = cmdData & 255;
      chanelBits = cmdData >> 8;
      if (0 === switchBits) {
        cmdData = status & (~chanelBits);
      }
      else {
        cmdData = status | (switchBits & chanelBits)
      }
    }
  }
  else if ("040B09050102" === deviceType || "040B09050112" === deviceType || "040B09050202" === deviceType) {
    if (OPEN_ALL_CMD === cmdData) {
      cmdData = 771;
    }
    else if (CLOSE_ALL_CMD === cmdData) {
      cmdData = 768;
    }
    else if (771 !== cmdData && 768 !== cmdData) {
      switchBits = cmdData & 255;
      chanelBits = cmdData >> 8;
      if (0 === switchBits) {
        cmdData = status & (~chanelBits);
      }
      else {
        cmdData = status | (switchBits & chanelBits)
      }
    }
  }
  else if ("040B09050103" === deviceType || "040B09050113" === deviceType || "040B09050203" === deviceType) {
    if (OPEN_ALL_CMD === cmdData) {
      cmdData = 1799;
    }
    else if (CLOSE_ALL_CMD === cmdData) {
      cmdData = 1792;
    }
    else if (1792 !== cmdData && 1799 !== cmdData) {
      switchBits = cmdData & 255;
      chanelBits = cmdData >> 8;
      if (0 === switchBits) {
        cmdData = status & (~chanelBits);
      }
      else {
        cmdData = status | (switchBits & chanelBits)
      }
    }
  }
  return cmdData;
};
var rebuildThermostatParameters = function (deviceInfo, parameters) {
  if (util.isNullOrUndefined(parameters["heat_mode"])) {
    parameters = {
      "heat_mode": deviceInfo.extra.items.heat_mode,
      "temp_heat": parameters.temp_heat
    }
  }
  else {
    parameters = {
      "heat_mode": parameters.heat_mode,
      "temp_heat": parameters.temp_heat
    }
  }
  return parameters;
};

var rebuildKMJLedParameters = function (deviceInfo, parameters) {
  parameters.subCmds = KMJ_LED_SCENE[parameters.scene];
  return parameters;
};

function Executor(conx, uuid, token, configurator) {
  this.devicesConf = {};
  VirtualDevice.call(this, conx, uuid, token, configurator);
  this.handleResponse = function (deviceInfo, cmd, response, callback) {
    var self = this;
    if (response.retCode === 200) {
      var result = response.data;
      var deviceType = deviceInfo.type.id;
      var cmdId = cmd.cmdCode;
      var deviceConf = self.devicesConf[deviceType];
      if (!util.isNullOrUndefined(deviceConf)
        && !util.isNullOrUndefined(deviceConf[cmdId])
        && !util.isNullOrUndefined(deviceConf[cmdId]["update"])
      ) {
        try {
          var updateConf = JSON.parse(deviceConf[cmdId]["update"]);
          updateConf = parseConf(updateConf, cmd.parameters, result);
          updateConf.uuid = deviceInfo.uuid;
          var msg = {
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
              cmdName: "deviceUpdate",
              cmdCode: "0004",
              parameters: updateConf
            }
          };
          logger.info(updateConf);
          self.message(msg, function (response) {
            if (response.retCode !== 200) {
              logger.error(response.retCode, response.description);
            }
          });
        }
        catch (e) {
          callback({errorId: 207000, errorMsg: e});
        }
      }
      callback(null, result);
    } else {
      callback({errorId: response.retCode, errorMsg: response.description});
    }
  }
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
          if (!util.isNullOrUndefined(message.userUuid)) {
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
          var found = _.findIndex(SUPPORTED_RECEIVER_TYPES, function (item) {
            return item === deviceType;
          });
          if (-1 !== found && "0001" === message.cmd.cmdCode) {//补全接收器命令码
            message.cmd.parameters.cmdData = receiverCmdCompleting(deviceInfo, message.cmd.parameters.cmdData);
          }
          //补全温控器设置温度的当前模式，以当前模式为准(注意结构顺序)
          if (HL_THERMOSTAT_TYPE === deviceType && "0003" === message.cmd.cmdCode) {
            message.cmd.parameters = rebuildThermostatParameters(deviceInfo, message.cmd.parameters);
          }
          //拆分LED灯场景控制命令
          /*if (KMJ_LED_TYPE === deviceType && "0009" === message.cmd.cmdCode) {
            message.cmd.parameters["subCmds"] = KMJ_LED_SCENE[message.cmd.parameters.scene];
          }*/
          if (!util.isNullOrUndefined(deviceConf)
            && !util.isNullOrUndefined(deviceConf[cmdId])
            && !util.isNullOrUndefined(deviceConf[cmdId]["parameters"])
          ) {
            try {
              var parametersConf = JSON.parse(deviceConf[cmdId]["parameters"]);
              var schema = parametersConf["properties"]["request"];
              self.messageValidate(message.cmd.parameters, schema, function (error) {
                if (error) {
                  logger.debug(schema);
                  logger.debug(message.cmd.parameters);
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
            if ("TCP" === protocol) {
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
            else if ("MQTT" === protocol) {
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
              //将场景命令封装为命令组
              if (KMJ_LED_TYPE === deviceInfo.type.id
                && message.cmd.cmdCode === "0009"
                && util.isArray(message.cmd.parameters.subCmds)) {
                msg = [];
                _.forEach(message.cmd.parameters.subCmds, function (subCmd) {
                  msg.push({
                    devices: [deviceInfo.uuid],
                    payload: subCmd
                  });
                })
              }
            }
            else {
              innerCallback({errorId: 207002, errorMsg: "Unsupported protocol type:" + protocol});
              return;
            }
          }
          else {
            innerCallback({
              errorId: 207003,
              errorMsg: "no command config:[name:" + message.cmd.cmdName + ",code:" + message.cmd.cmdCode
            });
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
          logger.info(msg);
          if (util.isArray(msg)) {
            async.mapSeries(msg,
              function (subMsg, callback) {
                self.message(subMsg, function (response) {
                  self.handleResponse(deviceInfo, subMsg.payload, response, function (error, result) {
                    if (error) {
                      callback(error);
                    }
                    else {
                      callback(null, result);
                    }
                  });
                });
              },
              function (error, result) {
                if (error) {
                  innerCallback(error);
                }
                else {
                  innerCallback(null, result);
                }
              });
          }
          else {
            self.message(msg, function (response) {
              self.handleResponse(deviceInfo, message.cmd, response, function (error, result) {
                if (error) {
                  innerCallback(error);
                }
                else {
                  innerCallback(null, result);
                }
              });
            });
          }
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
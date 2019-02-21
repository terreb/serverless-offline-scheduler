"use strict";

const BbPromise = require("bluebird");
const path = require("path");
const fs = require("fs");
const schedule = require("node-schedule");
const utils = require("./utils");
const {get} = require('lodash/fp');
const figures = require('figures');

const {createHandler, getFunctionOptions} = require('serverless-offline-python/src/functionHelper');
const createLambdaContext = require('serverless-offline-python/src/createLambdaContext');

const DEFAULT_TIMEOUT = 6;
const MS_PER_SEC = 1000;

class EventData {
  constructor(funcName, cron, rawEvent) {
    const event = rawEvent || {};

    this.name = funcName;
    this.cron = cron;
    this.enabled = event.enabled === undefined || event.enabled === null ? true : !!event.enabled;
    if (event.input) {
      this.input = event.input;
    }
    this.ruleName = event.name || funcName;
  }
}

class Scheduler {
  constructor(serverless) {
    this.serverless = serverless;
    this.location = "";
    this.run = this.run.bind(this);
    this._getFuncConfigs = this._getFuncConfigs.bind(this);
  }

  run() {
    const offlinePlugin = this.serverless.pluginManager.getPlugins()
      .find((p) => p.constructor && p.constructor.name === "Offline");
    if (offlinePlugin) {
      this.location = offlinePlugin.options.location;
    }
    this.funcConfigs = this._getFuncConfigs();

    for (const i in this.funcConfigs) {
      const fConfig = this.funcConfigs[i];
      for (const j in fConfig.events) {
        const eventData = fConfig.events[j];
        this._setEnvironmentVars(fConfig.id); //TODO: Set this individually for each schedule

        if (!eventData.enabled) {
          this.serverless.cli.log(`scheduler: not scheduling ${fConfig.id}/${eventData.name} `
            + `with ${eventData.cron}, since it's disabled`);
          continue;
        }

        this.serverless.cli.log(`scheduler: scheduling ${fConfig.id}/${eventData.name} `
          + `with ${eventData.cron}`);
        schedule.scheduleJob(eventData.cron, () => {
          // const func = this._requireFunction(fConfig.id);
          // this.serverless.cli.log(JSON.stringify(eventData));
          // if (!func) {
          //   this.serverless.cli.log(`scheduler: unable to find source for ${fConfig.id}`);
          //   return;
          // }
          // this.serverless.cli.log(`scheduler: running scheduled job: ${fConfig.id}`);
          // func(
          //   this._getEvent(eventData),
          //   this._getContext(fConfig),
          //   () => {}
          // );
          
          const event = eventData;
          const {location = '.'} = this._getConfig(this.serverless.service, 'serverless-offline');
          const functionName = fConfig.id;
          const __function = this.serverless.service.getFunction(functionName);
          const {env} = process;
          const functionEnv = Object.assign(
            {},
            env,
            get('service.provider.environment', this),
            get('environment', __function)
          );
          process.env = functionEnv;
          const servicePath = path.join(this.serverless.config.servicePath, location);
          const serviceRuntime = this.serverless.service.provider.runtime;
          const config = this._getConfig(this.serverless.service, 'serverless-offline-scheduler');
          const funOptions = getFunctionOptions(__function, functionName, servicePath, serviceRuntime);
          const handler = createHandler(funOptions, Object.assign({}, this.options, config));
          const lambdaContext = createLambdaContext(__function, (err, data) => {
            this.serverless.cli.log(
              `[${err ? figures.cross : figures.tick}] ${JSON.stringify(data) || ''}`
            );
            // cb(err, data);
          });

          if (handler.length < 3) {
            // handler(event, lambdaContext)
            //   .then(res => lambdaContext.done(null, res))
            //   .catch(lambdaContext.done);
            try {
              // TODO: test with Node
              const x = handler(event, lambdaContext);
      
              // Promise support
              if (serviceRuntime === 'nodejs8.10' || serviceRuntime === 'babel') {
                if (x && typeof x.then === 'function' && typeof x.catch === 'function') {
                  x.then(res => lambdaContext.done(null, res)).catch(lambdaContext.done);
                } else if (x instanceof Error) {
                  lambdaContext.done(x);
                }
              }
            } catch (error) {
              lambdaContext.done(error);
            }
          } else {
            handler(event, lambdaContext, lambdaContext.done);
          }     
        });
      }
    }
    return BbPromise.resolve();
  }

  _getConfig(service, pluginName) {
    return (service && service.custom && service.custom[pluginName]) || {};
  };

  _requireFunction(fName) {
    const functionObj = this.serverless.service.getFunction(fName);
    const handlerParts = functionObj.handler.split(".");
    const filename = `${handlerParts[0] }.js`;
    const handlerFunction = handlerParts[1];
    const funcPath = path.join(
      this.serverless.config.servicePath,
      this.location || "", filename);
    if (fs.existsSync(funcPath)) {
      delete require.cache[require.resolve(funcPath)];
      return require(funcPath)[handlerFunction];
    }
    return null;
  }

  _setEnvironmentVars(functionName) {
    const baseEnv = {
      IS_LOCAL: true,
      IS_OFFLINE: true
    };

    const providerEnvVars = this.serverless.service.provider.environment || {};
    const functionEnvVars = this.serverless.service.functions[functionName].environment || {};

    Object.assign(process.env, baseEnv, providerEnvVars, functionEnvVars);
  }

  _getEvent(args) {
    if (args.input) {
      return args.input;
    }

    return {
      "account": "123456789012",
      "region": "serverless-offline",
      "detail": {},
      "detail-type": "Scheduled Event",
      "source": "aws.events",
      "time": new Date().toISOString(),
      "id": utils.guid(),
      "resources": [
        `arn:aws:events:serverless-offline:123456789012:rule/${args.ruleName}`
      ],
      "isOffline": true,
      "stageVariables": this.serverless.service.custom
        && this.serverless.service.custom.stageVariables
    };
  }

  _getContext(fConfig) {

    const functionName = fConfig.id;

    const timeout = fConfig.timeout || this.serverless.service.provider.timeout || DEFAULT_TIMEOUT;

    const endTime = Math.max(0, Date.now() + timeout * MS_PER_SEC);

    return {
      awsRequestId: utils.guid(),
      invokeid: utils.guid(),
      logGroupName: `/aws/lambda/${functionName}`,
      logStreamName: "2016/02/14/[HEAD]13370a84ca4ed8b77c427af260",
      functionVersion: "$LATEST",
      isDefaultFunctionVersion: true,
      functionName,
      memoryLimitInMB: "1024",
      callbackWaitsForEmptyEventLoop: true,
      invokedFunctionArn: `arn:aws:lambda:serverless-offline:123456789012:function:${functionName}`,
      getRemainingTimeInMillis: () => endTime - Date.now()
    };
  }

  _convertRateToCron(rate) {
    const parts = rate.split(" ");
    if (!parts[1]) {
      this.serverless.cli.log(`scheduler: Invalid rate syntax '${rate}', will not schedule`);
      return null;
    }

    if (parts[1].startsWith("minute")) {
      return `*/${parts[0]} * * * *`;
    }

    if (parts[1].startsWith("hour")) {
      return `0 */${parts[0]} * * *`;
    }

    if (parts[1].startsWith("day")) {
      return `0 0 */${parts[0]} * *`;
    }

    this.serverless.cli.log(`scheduler: Invalid rate syntax '${rate}', will not schedule`);
    return null;
  }

  _convertCronSyntax(cronString) {
    const CRON_LENGTH_WITH_YEAR = 6;
    if (cronString.split(" ").length < CRON_LENGTH_WITH_YEAR) {
      return cronString;
    }

    return cronString.replace(/\s\S+$/, "");
  }

  _convertExpressionToCron(scheduleEvent) {
    const params = scheduleEvent
      .replace("rate(", "")
      .replace("cron(", "")
      .replace(")", "");

    if (scheduleEvent.startsWith("cron(")) {
      return this._convertCronSyntax(params);
    }
    if (scheduleEvent.startsWith("rate(")) {
      return this._convertRateToCron(params);
    }

    this.serverless.cli.log("scheduler: invalid, schedule syntax");
    return null;
  }

  _getFuncConfigs() {
    const funcConfs = [];
    const inputfuncConfs = this.serverless.service.functions;

    for (const funcName in inputfuncConfs) {
      const funcConf = inputfuncConfs[funcName];
      const scheduleEvents = funcConf.events
        .filter((e) => e.hasOwnProperty("schedule"))
        .map((e) => this._parseEvent(funcName, e.schedule))
        .filter((s) => s);
      if (scheduleEvents.length > 0) {
        funcConfs.push({
          id: funcName,
          events: scheduleEvents,
          timeout: funcConf.timeout,
          moduleName: funcConf.handler.split(".")[0]
        });
      }
    }
    return funcConfs;
  }

  _parseScheduleObject(funcName, rawEvent) {
    return new EventData(
      funcName,
      this._convertExpressionToCron(rawEvent.rate),
      rawEvent
      );
  }

  _parseScheduleExpression(funcName, expression) {
    return new EventData(funcName, this._convertExpressionToCron(expression));
  }

  _parseEvent(funcName, rawEvent) {
    if (typeof rawEvent === "string") {
      return this._parseScheduleExpression(funcName, rawEvent);
    }
    return this._parseScheduleObject(funcName, rawEvent);
  }
}
module.exports = Scheduler;

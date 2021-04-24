'use strict';
const path = require('path');

const getSuccessMessage = function(connectorName, outputPath, additionalMessage){
    return `
🚀 🚀 🚀 🚀 🚀 🚀

Success! 

Your ${connectorName} connector has been created at ${path.resolve(outputPath)}.

Follow the TODOs in the generated module to implement your connector. 

Questions, comments, or concerns? Let us know at:
Slack: https://slack.airbyte.io
Github: https://github.com/airbytehq/airbyte

We're always happy to provide any support!

${additionalMessage || ""}
`
}

module.exports = function (plop) {
  const pythonSourceInputRoot = '../source-python';
  const singerSourceInputRoot = '../source-singer';
  const genericSourceInputRoot = '../source-generic';
  const httpApiInputRoot = '../source-python-cdk';

  const outputDir = '../../connectors';
  const pythonSourceOutputRoot = `${outputDir}/source-{{dashCase name}}`;
  const singerSourceOutputRoot = `${outputDir}/source-{{dashCase name}}-singer`;
  const genericSourceOutputRoot = `${outputDir}/source-{{dashCase name}}`;
  const httpApiOutputRoot = `${outputDir}/source-{{dashCase name}}`;

  plop.setActionType('emitSuccess', function(answers, config, plopApi){
      console.log(getSuccessMessage(answers.name, plopApi.renderString(config.outputPath, answers), config.message));
  });

  plop.setGenerator('Python HTTP API Source', {
    description: 'Generate a Source that pulls data from a synchronous HTTP API.',
    prompts: [{type: 'input', name: 'name', message: 'Source name e.g: "google-analytics"'}],
    actions: [
      {
        abortOnFail: true,
        type:'addMany',
        destination: httpApiOutputRoot,
        base: httpApiInputRoot,
        templateFiles: `${httpApiInputRoot}/**/**`,
      },
      // plop doesn't add dotfiles by default so we manually add them
      {
        type:'add',
        abortOnFail: true,
        templateFile: `${httpApiInputRoot}/.dockerignore.hbs`,
        path: `${httpApiOutputRoot}/.dockerignore`
      },
      {type: 'emitSuccess', outputPath: httpApiOutputRoot}
    ]
  });

  plop.setGenerator('Python Singer Source', {
    description: 'Generate a Singer-tap-based Airbyte Source.',
    prompts: [
      {type: 'input', name: 'name', message: 'Source name, without the "source-" prefix e.g: "google-analytics"', filter: function (name) {
        return name.endsWith('-singer') ? name.replace(/-singer$/, '') : name;
      }},
       {type: 'input', name: 'tap_name', message: 'Singer tap package e.g "tap-mixpanel"'},
    ],
    actions: [
       {
         abortOnFail: true,
         type:'addMany',
         destination: singerSourceOutputRoot,
         base: singerSourceInputRoot,
         templateFiles: `${singerSourceInputRoot}/**/**`,
       },
       {
         type:'add',
         abortOnFail: true,
         templateFile: `${singerSourceInputRoot}/.gitignore.hbs`,
         path: `${singerSourceOutputRoot}/.gitignore`
       },
       {
         type:'add',
         abortOnFail: true,
         templateFile: `${singerSourceInputRoot}/.dockerignore.hbs`,
         path: `${singerSourceOutputRoot}/.dockerignore`
       },
        {type: 'emitSuccess', outputPath: singerSourceOutputRoot},
    ]
  });

    plop.setGenerator('Python Source', {
        description: 'Generate a minimal Python Airbyte Source Connector that works with any kind of data source. Use this if none of the other Python templates serve your use case.',
        prompts: [{type: 'input', name: 'name', message: 'Source name, without the "source-" prefix e.g: "google-analytics"'}],
        actions: [
            {
                abortOnFail: true,
                type:'addMany',
                destination: pythonSourceOutputRoot,
                base: pythonSourceInputRoot,
                templateFiles: `${pythonSourceInputRoot}/**/**`,
            },
            {
                type:'add',
                abortOnFail: true,
                templateFile: `${pythonSourceInputRoot}/.gitignore.hbs`,
                path: `${pythonSourceOutputRoot}/.gitignore`
            },
            {
                type:'add',
                abortOnFail: true,
                templateFile: `${pythonSourceInputRoot}/.dockerignore.hbs`,
                path: `${pythonSourceOutputRoot}/.dockerignore`
            },
            {type: 'emitSuccess', outputPath: pythonSourceOutputRoot, message: "For a checklist of what to do next go to https://docs.airbyte.io/tutorials/building-a-python-source"}]
    });

  plop.setGenerator('Generic Source', {
      description: 'Use if none of the other templates apply to your use case.',
      prompts: [{type: 'input', name: 'name', message: 'Source name, without the "source-" prefix e.g: "google-analytics"'}],
      actions: [
        {
          abortOnFail: true,
          type:'addMany',
          destination: genericSourceOutputRoot,
          base: genericSourceInputRoot,
          templateFiles: `${genericSourceInputRoot}/**/**`,
        },
        {
          type:'add',
          abortOnFail: true,
          templateFile: `${genericSourceInputRoot}/.gitignore.hbs`,
          path: `${genericSourceOutputRoot}/.gitignore`
        },
          {type: 'emitSuccess', outputPath: genericSourceOutputRoot}
      ]
    });
};

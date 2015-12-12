'use strict';

import * as path from 'path';
import * as fs from 'fs-promise';
import * as Q from 'q';
import { ply, $, Expression, ExpressionJS, RefExpression, ChainExpression, External, Datum, Dataset, TimeRange,
         basicExecutorFactory, Executor, AttributeJSs, AttributeInfo, Attributes } from 'plywood';
import { DataSource } from '../../../common/models/index';
import { parseData } from '../../../common/utils/parser/parser';

/**
 * Look for all instances of countDistinct($blah) and return the blahs
 * @param ex
 * @returns {string[]}
 */
function getCountDistinctReferences(ex: Expression): string[] {
  var references: string[] = [];
  ex.forEach((ex: Expression) => {
    if (ex instanceof ChainExpression) {
      var actions = ex.actions;
      for (var action of actions) {
        if (action.action === 'countDistinct') {
          var refExpression = action.expression;
          if (refExpression instanceof RefExpression) references.push(refExpression.name);
        }
      }
    }
  });
  return references;
}

export function getFileData(filePath: string): Q.Promise<any[]> {
  return fs.readFile(filePath, 'utf-8').then((fileData) => {
    try {
      return parseData(fileData, path.extname(filePath));
    } catch (e) {
      throw new Error(`could not parse '${filePath}': ${e.message}`);
    }
  }).then((fileJSON) => {
    fileJSON.forEach((d: Datum, i: number) => {
      d['time'] = new Date(d['time']);
    });
    return fileJSON;
  });
}

export function externalFactory(dataSource: DataSource, druidRequester: Requester.PlywoodRequester<any>, timeout: number, useSegmentMetadata: boolean): Q.Promise<External> {
  var filter: ExpressionJS = null;
  if (dataSource.subsetFilter) {
    filter = dataSource.subsetFilter.toJS();
  }

  var context = {
    timeout
  };

  if (dataSource.introspection === 'none') {
    //return Q(External.fromJS({
    //  engine: 'druid',
    //  dataSource: dataSource.source,
    //  timeAttribute: dataSource.timeAttribute.name,
    //  customAggregations: dataSource.options['customAggregations'],
    //  attributes: deduceAttributes(dataSource),
    //  useSegmentMetadata,
    //  filter,
    //  context: null,
    //  requester: druidRequester
    //}));
  } else {
    return External.fromJS({
      engine: 'druid',
      dataSource: dataSource.source,
      timeAttribute: dataSource.timeAttribute.name,
      attributeOverrides: dataSource.options['attributeOverrides'],
      customAggregations: dataSource.options['customAggregations'],
      useSegmentMetadata,
      filter,
      context,
      requester: druidRequester
    }).introspect();
  }
}

export function dataSourceFillerFactory(druidRequester: Requester.PlywoodRequester<any>, fileDirectory: string, timeout: number, useSegmentMetadata: boolean) {
  return function(dataSource: DataSource): Q.Promise<DataSource> {
    switch (dataSource.engine) {
      case 'native':
        // Do not do anything if the file was already loaded
        if (dataSource.executor) return Q(dataSource);

        if (!fileDirectory) {
          throw new Error('Must have a file directory');
        }

        var filePath = path.join(fileDirectory, dataSource.source);
        return getFileData(filePath).then((rawData) => {
          var dataset = Dataset.fromJS(rawData).hide();
          dataset.introspect();

          if (dataSource.subsetFilter) {
            dataset = dataset.filter(dataSource.subsetFilter.getFn(), {});
          }

          var executor = basicExecutorFactory({
            datasets: { main: dataset }
          });

          return dataSource.addAttributes(dataset.attributes).attachExecutor(executor);
        });

      case 'druid':
        return externalFactory(dataSource, druidRequester, timeout, useSegmentMetadata).then((external) => {
          var executor = basicExecutorFactory({
            datasets: { main: external }
          });

          return dataSource.addAttributes(external.attributes).attachExecutor(executor);
        }).then(DataSource.updateMaxTime);

      default:
        throw new Error(`Invalid engine: '${dataSource.engine}' in '${dataSource.name}'`);
    }
  };
}

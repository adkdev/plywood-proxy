'use strict';

import * as Q from 'q';
import { List, OrderedSet } from 'immutable';
import { Class, Instance, isInstanceOf, arraysEqual } from 'immutable-class';
import { Duration, Timezone, minute, second } from 'chronoshift';
import { ply, $, Expression, ExpressionJS, Executor, RefExpression, basicExecutorFactory, Dataset, Datum, Attributes, AttributeInfo, ChainExpression, SortAction } from 'plywood';
import { makeTitle, listsEqual } from '../../utils/general/general';
import { MaxTime, MaxTimeJS } from '../max-time/max-time';
import { RefreshRule, RefreshRuleJS } from '../refresh-rule/refresh-rule';

function formatTimeDiff(diff: number): string {
  diff = Math.round(Math.abs(diff) / 1000); // turn to seconds
  if (diff < 60) return 'less than 1 minute';

  diff = Math.floor(diff / 60); // turn to minutes
  if (diff === 1) return '1 minute';
  if (diff < 60) return diff + ' minutes';

  diff = Math.floor(diff / 60); // turn to hours
  if (diff === 1) return '1 hour';
  if (diff <= 24) return diff + ' hours';

  diff = Math.floor(diff / 24); // turn to days
  return diff + ' days';
}

export interface DataSourceValue {
  name: string;
  title?: string;
  engine: string;
  source: string;
  subsetFilter?: Expression;
  options?: Lookup<any>;
  introspection: string;
  timeAttribute: RefExpression;
  defaultTimezone: Timezone;
  defaultDuration: Duration;
  refreshRule: RefreshRule;
  maxTime?: MaxTime;

  executor?: Executor;
}

export interface DataSourceJS {
  name: string;
  title?: string;
  engine: string;
  source: string;
  subsetFilter?: ExpressionJS;
  options?: Lookup<any>;
  introspection?: string;
  timeAttribute?: string;
  defaultTimezone?: string;
  defaultDuration?: string;
  refreshRule?: RefreshRuleJS;
  maxTime?: MaxTimeJS;
}

var check: Class<DataSourceValue, DataSourceJS>;
export class DataSource implements Instance<DataSourceValue, DataSourceJS> {
  static DEFAULT_INTROSPECTION = 'autofill-all';
  static INTROSPECTION_VALUES = ['none', 'no-autofill', 'autofill-dimensions-only', 'autofill-measures-only', 'autofill-all'];
  static DEFAULT_TIMEZONE = Timezone.UTC;
  static DEFAULT_DURATION = Duration.fromJS('P3D');

  static isDataSource(candidate: any): boolean {
    return isInstanceOf(candidate, DataSource);
  }

  static updateMaxTime(dataSource: DataSource): Q.Promise<DataSource> {
    if (!dataSource.shouldQueryMaxTime()) return Q(dataSource);

    var ex = ply().apply('maxTime', $('main').max(dataSource.timeAttribute));

    return dataSource.executor(ex).then((dataset: Dataset) => {
      var maxTimeDate = dataset.data[0]['maxTime'];
      if (!isNaN(maxTimeDate)) {
        return dataSource.changeMaxTime(MaxTime.fromDate(maxTimeDate));
      }
      return dataSource;
    });
  }

  static fromJS(parameters: DataSourceJS, executor: Executor = null): DataSource {
    var engine = parameters.engine;
    var timeAttributeName = parameters.timeAttribute;
    if (engine === 'druid' && !timeAttributeName) {
      timeAttributeName = 'time';
    }
    var timeAttribute = timeAttributeName ? $(timeAttributeName) : null;

    var introspection = parameters.introspection;

    // Back compat.
    var options = parameters.options || {};
    if (options['skipIntrospection']) {
      if (!introspection) introspection = 'none';
      delete options['skipIntrospection'];
    }
    if (options['disableAutofill']) {
      if (!introspection) introspection = 'no-autofill';
      delete options['disableAutofill'];
    }

    introspection = introspection || DataSource.DEFAULT_INTROSPECTION;
    if (DataSource.INTROSPECTION_VALUES.indexOf(introspection) === -1) {
      throw new Error(`invalid introspection value ${introspection}, must be one of ${DataSource.INTROSPECTION_VALUES.join(', ')}`);
    }

    var value: DataSourceValue = {
      executor: null,
      name: parameters.name,
      title: parameters.title,
      engine,
      source: parameters.source,
      subsetFilter: parameters.subsetFilter ? Expression.fromJSLoose(parameters.subsetFilter) : null,
      options,
      introspection,
      timeAttribute,
      defaultTimezone: parameters.defaultTimezone ? Timezone.fromJS(parameters.defaultTimezone) : DataSource.DEFAULT_TIMEZONE,
      defaultDuration: parameters.defaultDuration ? Duration.fromJS(parameters.defaultDuration) : DataSource.DEFAULT_DURATION,
      refreshRule: parameters.refreshRule ? RefreshRule.fromJS(parameters.refreshRule) : RefreshRule.query(),
      maxTime: parameters.maxTime ? MaxTime.fromJS(parameters.maxTime) : null
    };
    if (executor) {
      value.executor = executor;
    }
    return new DataSource(value);
  }


  public name: string;
  public title: string;
  public engine: string;
  public source: string;
  public subsetFilter: Expression;
  public options: Lookup<any>;
  public introspection: string;
  public timeAttribute: RefExpression;
  public defaultTimezone: Timezone;
  public defaultDuration: Duration;
  public refreshRule: RefreshRule;
  public maxTime: MaxTime;

  public executor: Executor;

  constructor(parameters: DataSourceValue) {
    var name = parameters.name;
    this.name = name;
    this.title = parameters.title || makeTitle(name);
    this.engine = parameters.engine;
    this.source = parameters.source;
    this.subsetFilter = parameters.subsetFilter;
    this.options = parameters.options || {};
    this.introspection = parameters.introspection || DataSource.DEFAULT_INTROSPECTION;
    this.timeAttribute = parameters.timeAttribute;
    this.defaultTimezone = parameters.defaultTimezone;
    this.defaultDuration = parameters.defaultDuration;
    this.refreshRule = parameters.refreshRule;
    this.maxTime = parameters.maxTime;

    this.executor = parameters.executor;
  }

  public valueOf(): DataSourceValue {
    var value: DataSourceValue = {
      name: this.name,
      title: this.title,
      engine: this.engine,
      source: this.source,
      subsetFilter: this.subsetFilter,
      options: this.options,
      introspection: this.introspection,
      timeAttribute: this.timeAttribute,
      defaultTimezone: this.defaultTimezone,
      defaultDuration: this.defaultDuration,
      refreshRule: this.refreshRule,
      maxTime: this.maxTime
    };
    if (this.executor) {
      value.executor = this.executor;
    }
    return value;
  }

  public toJS(): DataSourceJS {
    var js: DataSourceJS = {
      name: this.name,
      title: this.title,
      engine: this.engine,
      source: this.source,
      subsetFilter: this.subsetFilter ? this.subsetFilter.toJS() : null,
      introspection: this.introspection,
      defaultTimezone: this.defaultTimezone.toJS(),
      defaultDuration: this.defaultDuration.toJS(),
      refreshRule: this.refreshRule.toJS()
    };
    if (this.timeAttribute) {
      js.timeAttribute = this.timeAttribute.name;
    }
    if (Object.keys(this.options).length) {
      js.options = this.options;
    }
    if (this.maxTime) {
      js.maxTime = this.maxTime.toJS();
    }
    return js;
  }

  public toJSON(): DataSourceJS {
    return this.toJS();
  }

  public toString(): string {
    return `[DataSource: ${this.name}]`;
  }

  public equals(other: DataSource): boolean {
    return DataSource.isDataSource(other) &&
      this.name === other.name &&
      this.title === other.title &&
      this.engine === other.engine &&
      this.source === other.source &&
      Boolean(this.subsetFilter) === Boolean(other.subsetFilter) &&
      (!this.subsetFilter || this.subsetFilter.equals(other.subsetFilter)) &&
      JSON.stringify(this.options) === JSON.stringify(other.options) &&
      this.introspection === other.introspection &&
      Boolean(this.timeAttribute) === Boolean(other.timeAttribute) &&
      (!this.timeAttribute || this.timeAttribute.equals(other.timeAttribute)) &&
      this.defaultTimezone.equals(other.defaultTimezone) &&
      this.defaultDuration.equals(other.defaultDuration) &&
      this.refreshRule.equals(other.refreshRule);
  }

  public attachExecutor(executor: Executor): DataSource {
    var value = this.valueOf();
    value.executor = executor;
    return new DataSource(value);
  }

  public toClientDataSource(): DataSource {
    var value = this.valueOf();
    value.subsetFilter = null;
    value.introspection = 'none';
    return new DataSource(value);
  }

  public isQueryable(): boolean {
    return Boolean(this.executor);
  }

  public getMaxTimeDate(): Date {
    var { refreshRule } = this;
    if (refreshRule.rule === 'realtime') {
      return minute.ceil(new Date(), Timezone.UTC);
    } else if (refreshRule.rule === 'fixed') {
      return refreshRule.time;
    } else { //refreshRule.rule === 'query'
      var { maxTime } = this;
      if (!maxTime) return null;
      return second.ceil(maxTime.time, Timezone.UTC);
    }
  }

  public updatedText(): string {
    var { refreshRule } = this;
    if (refreshRule.rule === 'realtime') {
      return 'Updated: ~1 second ago';
    } else if (refreshRule.rule === 'fixed') {
      return `Fixed to: ${formatTimeDiff(Date.now() - refreshRule.time.valueOf())}`;
    } else { //refreshRule.rule === 'query'
      var { maxTime } = this;
      if (maxTime) {
        return `Updated: ${formatTimeDiff(Date.now() - maxTime.time.valueOf())} ago`;
      } else {
        return null;
      }
    }
  }

  public shouldQueryMaxTime(): boolean {
    if (!this.executor) return false;
    return this.refreshRule.shouldQuery(this.maxTime);
  }

  public isTimeAttribute(ex: Expression) {
    var { timeAttribute } = this;
    return ex.equals(this.timeAttribute);
  }

  public addAttributes(attributes: Attributes): DataSource {
    var { introspection } = this;
    if (introspection === 'none' || introspection === 'no-autofill') return this;

    var value = this.valueOf();
    value.introspection = 'no-autofill';

    return new DataSource(value);
  }

  public changeMaxTime(maxTime: MaxTime) {
    var value = this.valueOf();
    value.maxTime = maxTime;
    return new DataSource(value);
  }
}
check = DataSource;

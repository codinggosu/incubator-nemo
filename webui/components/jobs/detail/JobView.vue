<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
  <!--(toggle for debugging)-->
  <el-card v-if="selectedJobId" style="background-color: ghostwhite;" shadow="never">
  <!--<el-card>-->
    <h1>Details for Job {{ jobFrom ? jobFrom : 'NULL' }}</h1>

    <p>
      <b>Status: </b>
      <el-tag :type="_fromJobStatusToType(selectedJobStatus)">{{ selectedJobStatus }}</el-tag><br>
    </p>

    <el-collapse accordion @change="handleCollapse">
      <!--Event Timeline-->
      <!--
      <el-collapse-item title="  Event Timeline" name="1">
        <el-card header="Timeline" class="detail-card">
          <metric-timeline
            ref="metricTimeline"
            :selectedJobId="selectedJobId"
            :groups="groupDataSet"/>
        </el-card>
        <el-row :gutter="10">
          <el-col height="100%" :span="12" :xs="24">
            <el-card class="detail-card" header="Select stage">
              <stage-select
                :selectedJobId="selectedJobId"
                :metricLookupMap="metricLookupMap"/>
            </el-card>
          </el-col>
          <el-col :span="12" :xs="24">
            <el-card class="detail-card" header="Detail">
              <detail-table
                :tableData="tableData"/>
            </el-card>
          </el-col>
        </el-row>
      </el-collapse-item>
      -->
      <!--DAG Visualization-->
      <el-collapse-item title="  DAG Visualization" name="2">
        <no-ssr>
          <affix relative-element-selector="#affix-target" style="z-index: 1">
            <el-popover v-model="showdetail" trigger="manual" width="400">
              <el-button style="float: right;" size="mini" icon="el-icon-close" circle
                         @click="showdetail = false"></el-button>
              <detail-table :tableData="tableData"/>
            </el-popover>
          </affix>
        </no-ssr>
        <el-card header="DAG">
          <dag :selectedJobId="selectedJobId" :tabIndex="tabIndex" id="affix-target"/>
        </el-card>
      </el-collapse-item>
      <!--Tasks information-->
      <el-collapse-item title="  Task Statistics" name="3">
        <task-statistics
          :selectedJobId="selectedJobId"
          :taskStatistics="selectedTaskStatistics"/>
      </el-collapse-item>
    </el-collapse>

    <!--Stages List-->
    <h2 ref="stages">Stages
      <el-badge type="info" :value="stageList.length"></el-badge></h2>
    <div>
      <!--<div v-if="pendingStagesData.length !== 0">-->
      <el-table class="pending-stages-table" :data="stageList" stripe>
        <el-table-column label="Stage id">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="State">
          <template slot-scope="scope">
            <el-tag :type="_fromStageStatusToType(_getStageState(selectedTaskStatistics.stageState[scope.row]))">{{ _getStageState(selectedTaskStatistics.stageState[scope.row]) }}</el-tag><br>
          </template>
        </el-table-column>
      </el-table>
    </div>

  </el-card>
</template>

<script>
import Vue from 'vue';
import { DataSet } from 'vue2vis';
import MetricTimeline from './MetricTimeline'
import DetailTable from './DetailTable';
import StageSelect from './StageSelect';
import DAG from './DAG';
import TaskStatistics from '../../TaskStatistics';
import { STATE, JOB_STATUS } from '../../../assets/constants';

// list of metric, order of elements matters.
export const METRIC_LIST = [
  'StageMetric',
  'TaskMetric',
];

const LISTENING_EVENT_LIST = [
  'job-id-select',
  'job-id-deselect',
  'build-table-data',
  'metric-select',
  'metric-deselect',
];

export default {
  components: {
    'metric-timeline': MetricTimeline,
    'stage-select': StageSelect,
    'detail-table': DetailTable,
    'dag': DAG,
    'task-statistics': TaskStatistics,
  },

  props: ['selectedJobStatus', 'selectedJobMetricDataSet', 'selectedTaskStatistics'],

  data() {
    return {
      STATE: STATE,
      // timeline dataset
      groupDataSet: new DataSet([]),

      // selected metric id
      selectedMetricId: '',
      // selected job id
      selectedJobId: '',
      // endpoint or file name of job
      jobFrom: '',

      metricLookupMap: {}, // metricId -> data

      // element-ui specific
      collapseActiveNames: ['timeline', 'dag'],
      tableData: [],
      tabIndex: '0',
      showdetail: false,
    }
  },

  // COMPUTED
  computed: {
    // All stages
    stageList() {
      return Object.keys(this.metricLookupMap).filter(id => /^Stage[0-9]+$/.test(id.trim()));
    },
  },

  // METHODS
  methods: {
    // event timeline, dag event handler
    handleCollapse(activatedElement) {
      if (activatedElement === "1") {
        this.$eventBus.$emit('set-timeline-items', this.selectedJobMetricDataSet);
        this.$eventBus.$emit('redraw-timeline');
      } else if (activatedElement === "2") {
        this.$eventBus.$emit('rerender-dag');
      }
    },

    // jump to the table
    jump(event, val) {
      switch (val) {
        case STATE.READY:
          this.$refs.pendingStages.scrollIntoView();
          break;
        case STATE.EXECUTING:
          this.$refs.activeStages.scrollIntoView();
          break;
        case STATE.COMPLETE:
          this.$refs.completedStages.scrollIntoView();
          break;
        case STATE.INCOMPLETE:
          this.$refs.skippedStages.scrollIntoView();
          break;
      }
    },

    /**
     * Set up event handlers for this component.
     */
    setUpEventHandlers() {
      // event handler for detecting change of job id
      this.$eventBus.$on('job-id-select', data => {
        this.$eventBus.$emit('clear-stage-select');
        this.selectedJobId = data.jobId;
        this.jobFrom = data.jobFrom;
        this.metricLookupMap = data.metricLookupMap;
        this.selectedMetricId = '';
      });

      this.$eventBus.$on('job-id-deselect', () => {
        this.$eventBus.$emit('set-timeline-items', new DataSet([]));
        this.$eventBus.$emit('clear-stage-select');
        this.selectedJobId = '';
        this.jobFrom = '';
        this.metricLookupMap = {};
        this.selectedMetricId = '';
      });

      this.$eventBus.$on('build-table-data', ({ metricId, jobId }) => {
        if (this.selectedJobId === jobId &&
          this.selectedMetricId === metricId) {
          this.buildTableData(metricId);
          this.showdetail = true;
        }
      });

      // event handler for individual metric selection
      this.$eventBus.$on('metric-select', metricId => {
        this.selectedMetricId = metricId;
        this.buildTableData(metricId);
        this.showdetail = true;
        this.$eventBus.$emit('metric-select-done');
      });

      // event handler for individual metric deselection
      this.$eventBus.$on('metric-deselect', async () => {
        this.tableData = [];
        this.selectedMetricId = '';
        await this.$nextTick();
        this.showdetail = false;
        this.$eventBus.$emit('metric-deselect-done');
      });
    },

    /**
     * Build table data which will be used in TaskStatistics component.
     * @param metricId id of metric. Used to lookup metricLookupMap.
     */
    buildTableData(metricId) {
      this.tableData = [];
      const metric = this._removeUnusedProperties(this.metricLookupMap[metricId]);
      Object.keys(metric).forEach(key => {
        if (typeof metric[key] === 'object') {
          if (key === 'executionProperties') {
            let executionPropertyArray = [];
            Object.keys(metric[key]).forEach(ep => {
              executionPropertyArray.push({
                key: ep,
                value: metric[key][ep],
              });
            });
            this.tableData.push({
              key: key,
              value: '',
              extra: executionPropertyArray,
            });
          }
        } else {
          let value = metric[key] === -1 ? 'N/A' : metric[key];
          if (value !== 'N/A' && key.toLowerCase().endsWith('bytes')) {
            value = this._bytesToHumanReadable(value);
          }
          this.tableData.push({
            key: key,
            value: value,
          });
        }
      });
    },

    _fromJobStatusToType(status) {
      switch (status) {
        case JOB_STATUS.RUNNING:
          return 'primary';
        case JOB_STATUS.COMPLETE:
          return 'success';
        case JOB_STATUS.FAILED:
          return 'danger';
        default:
          return 'info';
      }
    },
    _getStageState(status) {
      if (this.selectedJobStatus === JOB_STATUS.COMPLETE) {
        return STATE.COMPLETE
      }
      if (status == null) {
        return STATE.INCOMPLETE
      }
      return status
    },
    _fromStageStatusToType(status) {
      switch (status) {
        case STATE.INCOMPLETE:
          return 'primary';
        case STATE.COMPLETE:
          return 'success';
        case JOB_STATUS.FAILED:
          return 'danger';
        default:
          return 'info';
      }
    },

    _bytesToHumanReadable(bytes) {
      var i = bytes === 0 ? 0 :
        Math.floor(Math.log(bytes) / Math.log(1024));
      return (bytes / Math.pow(1024, i)).toFixed(2) * 1
        + ' ' + ['B', 'KB', 'MB', 'GB', 'TB'][i];
    },

    _removeUnusedProperties(metric) {
      let newMetric = Object.assign({}, metric);
      delete newMetric.group;
      delete newMetric.content;
      return newMetric;
    },
  },

  // HOOKS
  beforeMount() {
    // predefine group sets
    METRIC_LIST.forEach(metricType => {
      this.groupDataSet.add({
        id: metricType,
        content: metricType
      });
    });

    this.setUpEventHandlers();
  },

  beforeDestroy() {
    LISTENING_EVENT_LIST.forEach(e => {
      this.$eventBus.$off(e);
    });
  },
}
</script>

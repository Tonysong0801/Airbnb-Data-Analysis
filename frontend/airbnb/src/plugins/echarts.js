import Vue from 'vue';
import Echarts from 'vue-echarts';

import 'echarts/lib/chart/bar';
import 'echarts/lib/chart/line';

import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/grid';
import 'echarts/lib/component/toolbox';

Vue.component('eChart', Echarts);
<template>
    <GChart
      type="ColumnChart"
      :data="chartData"
      :options="chartOptions"
    />
  </template>
  
  
  <script>
  import axios from "axios";
  import { GChart } from 'vue-google-charts'
  
  export default {
    name: "sub_category_predict_review",
    data() {
      return {
        chartData:[],
        chartOptions: {
          hAxis: {
            title: 'feature',
          },
          vAxis: {
            title: 'importance',
          },
          height:600,
        },
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/sub_category_predict_review")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['feature','feature_importance_mean','feature_importance_std']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].feature, response.data[i].feature_importance_mean, response.data[i].feature_importance_std])
            }
            this.chartData = graph_data
          })
          .catch((error) => {
            console.log(error);
          });
          // console.log('complete_init')
        }
    },
    mounted() {
      this.init_data();
    },
  };
  </script>
  
  <style rel="stylesheet/scss" lang="scss">
  </style>
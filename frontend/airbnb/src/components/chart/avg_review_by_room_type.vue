<template>
    <GChart
      type="BarChart"
      :data="chartData"
      :options="chartOptions"
    />
  </template>
  
  
  <script>
  import axios from "axios";
  import { GChart } from 'vue-google-charts'
  
  export default {
    name: "avg_review_by_room_type",
    data() {
      return {
        chartData:[],
        chartOptions: {
          hAxis: {
            title: 'review scores',
            minValue: 0,
          },
          vAxis: {
            title: 'room type',
          },
          height:1500,
        },
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/avg_review_by_room_type")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['room_type','avg_overall','avg_accuracy','avg_cleanliness'
            ,'avg_checkin','avg_communication','avg_location','avg_value']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].room_type, response.data[i].avg_overall, response.data[i].avg_accuracy,
                response.data[i].avg_cleanliness, response.data[i].avg_checkin, response.data[i].avg_communication,
                response.data[i].avg_location, response.data[i].avg_value])
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
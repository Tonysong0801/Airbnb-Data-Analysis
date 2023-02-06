<template>
    <GChart
      type="GeoChart"
      :data="chartData"
      :options="chartOptions"
      :settings="chartSettings"
    />
  </template>
  
  
  <script>
  import axios from "axios";
  import { GChart } from 'vue-google-charts'
  
  export default {
    name: "priceWithSpace",
    data() {
      return {
        chartData:[],
        chartOptions: {
          height: 800,
          resolution: 'provinces',
          region: 'CA',
          displayMode: 'markers',
          magnifyingGlass:{enable: true, zoomFactor: 20},
        },
        chartSettings: {
          packages: ['geochart'],
        },
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/priceWithSpace")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['latitude','longitude','average_price']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].avg_latitute, response.data[i].avg_longitude, response.data[i].average_price])
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
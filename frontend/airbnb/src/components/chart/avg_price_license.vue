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
    name: "avg_price_license",
    data() {
      return {
        chartData:[],
        chartOptions: {
          
            // title: 'Correlation between bedrooms_num, beds_num and accommodates',
            hAxis: {
              title: 'Price',
              minValue: 0,
            },
            height: 200,
            colors: ['#43D7FF']
          
        }
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/avg_price_license")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['license','average price']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].license, response.data[i].avg_price])
            }
            this.chartData = graph_data
          })
          .catch((error) => {
            console.log(error);
          });
          console.log('complete_init')
        }
    },
    mounted() {
      this.init_data();
    },
  };
  </script>
  
  <style rel="stylesheet/scss" lang="scss">
  </style>
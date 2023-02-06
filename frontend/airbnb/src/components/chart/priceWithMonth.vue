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
    name: "priceWithMonth",
    data() {
      return {
        chartData:[],
        chartOptions: {
          
            // title: 'Correlation between bedrooms_num, beds_num and accommodates',
            hAxis: {
              title: 'Month',
            },
            vAxis: {
              title: 'Price',
            },
            height: 400,
            colors: ['#C3008E', '#E7A522']
          
        }
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/priceWithMonth")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['Month','average_price','median_price']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].month, response.data[i].average_price, response.data[i].median_price])
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
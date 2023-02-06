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
    name: "priceWithWeek",
    data() {
      return {
        chartData:[],
        chartOptions: {
          
            // title: 'Correlation between bedrooms_num, beds_num and accommodates',
            hAxis: {
              title: 'Week',
            },
            vAxis: {
              title: 'Price',
            },
            height: 400,
            colors: ['#F254FC', '#F7DB6A']
          
        }
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/priceWithWeek")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['week','average_price','median_price']]
            for (let i = 0; i < response_data.length; i++) {
                graph_data.push([response.data[i].week, response.data[i].average_price, response.data[i].median_price])
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
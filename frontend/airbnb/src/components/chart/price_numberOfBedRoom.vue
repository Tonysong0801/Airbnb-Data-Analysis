<template>
    <GChart
      type="CandlestickChart"
      :data="chartData"
      :options="chartOptions"
    />
  </template>
  
  
  <script>
  import axios from "axios";
  import { GChart } from 'vue-google-charts'
  
  export default {
    name: "price_numberOfBedRoom",
    data() {
      return {
        chartData:[],
        chartOptions: {
          
            // title: 'Correlation between bedrooms_num, beds_num and accommodates',
            vAxis: {
              title: 'Price',
            },
            hAxis: {
              title: 'number of bedrooms',
            },
            height: 600,
          
        }
      };
    },
    components: {
      GChart,
    },
    methods: {
        init_data(){
          axios
          .get("/api/price_numberOfBedRoom")
          .then((response) => {
            var response_data = response.data;
            var graph_data = [['bedrooms_nums','min-max, Q1-Q3','q1','q3','max']]
            for (let i = 0; i < response_data.length; i++) {
                if (response_data[i].bedrooms_nums==-1){
                    response.data[i].bedrooms_nums='over_5'
                }
                graph_data.push([response.data[i].bedrooms_nums.toString()+'\nmedian:'+response.data[i].median, response.data[i].min, response.data[i].q1,
                response.data[i].q3, response.data[i].max])
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
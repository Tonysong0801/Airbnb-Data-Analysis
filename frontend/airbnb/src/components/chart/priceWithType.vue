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
  name: "priceWithType",
  data() {
    return {
      chartData:[],
      chartOptions: {
        
          // title: 'Correlation between bedrooms_num, beds_num and accommodates',
          hAxis: {
            title: 'room_type',
          },
          vAxis: {
            title: 'Price',
          },
          height: 400,
          colors: ['#0B96C3', '#0B43C3', '#F11573', '#0DAF15']
        
      }
    };
  },
  components: {
    GChart,
  },
  methods: {
      init_data(){
        axios
        .get("/api/priceWithType")
        .then((response) => {
          var response_data = response.data;
          var graph_data = [['room_type','average_price','median_price','max_price','min_price']]
          for (let i = 0; i < response_data.length; i++) {
              graph_data.push([response.data[i].room_type, response.data[i].average_price, response.data[i].median_price,
              response.data[i].max_price,response.data[i].min_price])
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
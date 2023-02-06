<template>
  <GChart
    type="Table"
    :data="chartData"
    :options="chartOptions"
  />
</template>


<script>
import axios from "axios";
import { GChart } from 'vue-google-charts'

export default {
  name: "corela_rooms_beds_accommodates",
  data() {
    return {
      chartData:[],
      chartOptions: {
        curveType: 'function',
          height: 400,
          width:800,
      }
    };
  },
  components: {
    GChart,
  },
  methods: {
      init_data(){
        axios
        .get("/api/corela_rooms_beds_accommodates")
        .then((response) => {
          var response_data = response.data;
          var graph_data = [['bedrooms','beds','accommodates']]
          for (let i = 0; i < response_data.length; i++) {
            graph_data.push([response.data[i].bedrooms, response.data[i].beds, response.data[i].accommodates])
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
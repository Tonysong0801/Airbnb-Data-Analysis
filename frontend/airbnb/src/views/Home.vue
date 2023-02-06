<template>
    <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Correlation between bedrooms_num, beds_num and accommodates</h2>
      </div>
    </template>
    <corela_rooms_beds_accommodates/>
  </el-card>

  <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Price Distribution by BedRooms_number</h2>
      </div>
    </template>
    <price_numberOfBedRoom/>
  </el-card>

  <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Average Price by having License(1) or not(0)</h2>
      </div>
    </template>
    <avg_price_license/>
  </el-card>

  <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Changes in the mean and median house prices in the Vancouver area by month</h2>
      </div>
    </template>
    <priceWithMonth/>
  </el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>Changes in the mean and median house prices in the Vancouver area by week</h2>
    </div>
  </template>
  <priceWithWeek/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>The average house price for each neighborhood in Canada and its average latitude and longitude</h2>
    </div>
  </template>
  <div><priceWithSpace class="price-space"/></div>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>Average, median, highest, and lowest prices for four types of homes in the Vancouver area</h2>
    </div>
  </template>
  <priceWithType/>
</el-card>


</template>

<script>
import corela_rooms_beds_accommodates from '../components/chart/corela_rooms_beds_accommodates';
import price_numberOfBedRoom from '../components/chart/price_numberOfBedRoom';
import avg_price_license from '../components/chart/avg_price_license';
import priceWithMonth from '../components/chart/priceWithMonth';
import priceWithWeek from '../components/chart/priceWithWeek';
import priceWithSpace from '../components/chart/priceWithSpace';
import priceWithType from '../components/chart/priceWithType';




export default {
  name: 'theHome',
  props: {
    msg: String
  },
  components: {
    corela_rooms_beds_accommodates,
    price_numberOfBedRoom,
    avg_price_license,
    priceWithMonth,
    priceWithWeek,
    priceWithSpace,
    priceWithType,

  },
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.box-card{
  margin:5%
}
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>

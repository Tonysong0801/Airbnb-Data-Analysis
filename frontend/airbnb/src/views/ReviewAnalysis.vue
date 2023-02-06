<template>
<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>avg review scores by city</h2>
    </div>
  </template>
  <avg_review_by_city/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>avg review scores by room type</h2>
    </div>
  </template>
  <avg_review_by_room_type/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>avg review scores by price bucket</h2>
    </div>
  </template>
  <avg_review_by_price_bucket/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>avg review scores for superhosts vs non-superhosts</h2>
    </div>
  </template>
  <avg_review_by_superhosts/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>sub-category review scores most significant in predicting overall review scores</h2>
    </div>
  </template>
  <sub_category_predict_review/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>other factors that are most significant in predicting overall review scores</h2>
    </div>
  </template>
  <factors_predict_review/>
</el-card>


<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>most frequent words used in positive reviews</h2>
    </div>
  </template>
  <img alt="image load failed" src="../img/pos.png"/>
</el-card>

<el-card class="box-card">
  <template #header>
    <div class="card-header">
      <h2>most frequent words used in negative reviews</h2>
    </div>
  </template>
  <img alt="image load failed" src="../img/neg.png"/>
</el-card>

</template>

<script>

import avg_review_by_city from '../components/chart/avg_review_by_city';
import avg_review_by_room_type from '../components/chart/avg_review_by_room_type';
import avg_review_by_price_bucket from '../components/chart/avg_review_by_price_bucket';
import avg_review_by_superhosts from '../components/chart/avg_review_by_superhosts';

import factors_predict_review from '../components/chart/factors_predict_review';
import sub_category_predict_review from '../components/chart/sub_category_predict_review';


export default {
  name: 'ReviewAnalysis',
  props: {
    msg: String
  },
  components: {
    avg_review_by_city,
    avg_review_by_room_type,
    avg_review_by_price_bucket,
    avg_review_by_superhosts,

    factors_predict_review,
    sub_category_predict_review,

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

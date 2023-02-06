<template>
  <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Recommend a house similar to your choice in Vancouver Area</h2>
      </div>
    </template>
    <div class="alert alert-danger" role="alert" v-if="errors.length">
      <p v-for="error in errors" :key="error" style="color:red;">{{error}}</p>
    </div>
   <el-form :model="form" label-width="15%">
    <el-form-item label="Similar house you are looking for">
      <el-input v-model="form.house_id" placeholder="please enter the id(eg.13490)"/>
    </el-form-item>

    <el-form-item label="Number of recommendation">
      <el-input v-model="form.nums" placeholder="we suggest to have a recommend number from 1 - 10"/>
    </el-form-item>

    <el-form-item>
      <el-button type="primary" @click="onSubmit">Recommend</el-button>
    </el-form-item>

   </el-form>
   <h2>result</h2>
   <p><span v-html="result">

   </span></p>

  </el-card>
    
    </template>
    
    <script>
    import { reactive } from 'vue'
    import axios from "axios";
    
    export default {
      name: 'theRecommend',
      components: {
    
      },
      data() {
        return {
          form : reactive({
            house_id: '',
            nums: '',
          }),
          errors: [],
          result: '',

        };
      },
      methods: {
        onSubmit(){
          // validate the data
          this.errors= []
          if (!(this.form.house_id && this.form.nums)){
            this.errors.push("please make sure to fill in all fields");
          }
          if (isNaN(this.form.house_id) || isNaN(this.form.nums)) {
            this.errors.push("please enter a number for the fields");
          }if (parseInt(this.form.nums)>11 || parseInt(this.form.nums)<1) {
            this.errors.push("we suggest to have a recommend number from 1 - 10");
          }
          if (!this.errors.length) {
          this.result = 'Please wait, the recommendation will cost some time...'
            var data = {
              id: this.form.house_id,
              nums: this.form.nums,
            }
            console.log(data)
            axios
              .post("/api/recommendation/", data)
              .then((response) => {
                console.log("response error:" + response.data.error);
                if (response.data.success) {
                  this.result = ''
                  for (let i=0;i<response.data.data.length; i++){
                    console.log("response:" + response.data.data[i].recommend);
                    this.result = this.result + '<br>--------------------------------------------------------------------<br>Recommended: ' + response.data.data[i].recommend + '<br>Score: ' + response.data.data[i].score;
                  }
                  
                }else{
                  this.errors.push("error:",response.data.e);
                }
              })
              .catch((error) => {
                console.log(error);
              });
          }
        }
      },
    }
    </script>
    

    <!-- Add "scoped" attribute to limit CSS to this component only -->
    <style scoped>
    .box-card{
      margin:3%;
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
    p{
      text-align: left;
    }
    h2{
      text-align: left;
    }
    </style>
    
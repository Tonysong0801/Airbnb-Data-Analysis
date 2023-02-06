<template>
  <el-card class="box-card">
    <template #header>
      <div class="card-header">
        <h2>Predict the Price in Toronto Area</h2>
      </div>
    </template>
    <div class="alert alert-danger" role="alert" v-if="errors.length">
      <p v-for="error in errors" :key="error" style="color:red;">{{error}}</p>
    </div>
   <el-form :model="form" label-width="15%" @submit.prevent>
    <el-form-item label="Number of accommodates">
      <el-input v-model="form.accommodates" />
    </el-form-item>

    <el-form-item label="Number of bedrooms">
      <el-input v-model="form.bedrooms" />
    </el-form-item>
    
    <el-form-item label="Number of beds">
      <el-input v-model="form.beds" />
    </el-form-item>
    
    <el-form-item label="Number of license">
      <el-input v-model="form.license" />
    </el-form-item>
    
    <el-form-item label="Number of bathrooms">
      <el-input v-model="form.bathrooms" />
    </el-form-item>

    <el-form-item>
      <el-button type="primary" @click="onSubmit">Predict Price</el-button>
    </el-form-item>

   </el-form>
   <h2>result</h2>
   <p>{{result}}</p>
  </el-card>
    
    
</template>
    
    <script>
    import { reactive } from 'vue'
    import axios from "axios";

    
    export default {
      name: 'PredictPrice',
      components: {
    
      },
      data() {
        return {
          // 'accommodates','bedrooms','beds','license','bathrooms','room_type','neighbourhood_cleansed'
          form : reactive({
            accommodates: '',
            bedrooms: '',
            beds:'',
            license:'',
            bathrooms:''
            }),
          errors: [],
          result:'',
        };
      },
      methods: {
        onSubmit(){
          this.errors= []
          // validate the data
          if (!(this.form.accommodates && this.form.bedrooms && this.form.beds && this.form.license && this.form.bathrooms)){
            this.errors.push("please make sure to fill in all fields");
          }
          if (isNaN(this.form.accommodates) || isNaN(this.form.bedrooms) || isNaN(this.form.beds) || isNaN(this.form.license) || isNaN(this.form.bathrooms)) {
            this.errors.push("please enter a number for the fields");
          }if (parseInt(this.form.license)>1 | parseInt(this.form.license)<0) {
            this.errors.push("please only enter 1 or 0 for the license field");
          }
          if (!this.errors.length) {
          this.result = 'Please wait, the prediction will cost some time...'
            var data = {
              accommodates: this.form.accommodates,
              bedrooms: this.form.bedrooms,
              beds: this.form.beds,
              license: this.form.license,
              bathrooms: this.form.bathrooms,
            }
            console.log(data)
            axios
              .post("/api/predict_price/", data)
              .then((response) => {
                console.log("response:" + response.data.error);
                if (response.data.success) {
                  console.log("response:" + response.data.data);
                  this.result = 'The predicted price is: '+response.data.data
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
    
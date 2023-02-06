<template>
  <div class="affix-container">
    <el-affix target=".affix-container">
      <el-menu
        :default-active="activeIndex"
        class="el-menu-demo"
        mode="horizontal"
        background-color="#545c64"
        text-color="#fff"
        active-text-color="#ffd04b"
        @select="handleSelect"
      >
        <el-menu-item index="1">
          <router-link to='/'>Housing Analysis</router-link>
        </el-menu-item>
        <el-menu-item index="2">
          <router-link to='/review_analysis'>Reviews Analysis</router-link>
        </el-menu-item>
        <el-sub-menu index="3">
          <template #title>Advanced Features</template>
          <el-menu-item index="2-1"><router-link to='/predict_price' style="color: aliceblue;">Predict Price</router-link></el-menu-item>
          <el-menu-item index="2-2"><router-link to='/recommend' style="color: aliceblue;">House Recommendation</router-link></el-menu-item>
        </el-sub-menu>
      </el-menu>
    </el-affix>
    <router-view></router-view>
  <!-- <Home msg="Welcome to Your Vue.js App"/> -->
  </div>
</template>

<script>
// import theHome from './views/Home.vue'
import { ref } from 'vue'

export default {
  name: 'App',
  data() {
    return {
      activeIndex : ref('1'),

    };
  },
  components: {
    // theHome
  },
  methods: {
    handleSelect(key, keyPath) {
      // if(key==1){
      //   this.$router.push('/')
      // }
      // if(key==2){
      //   this.$router.push('/review_analysis')
      // }
      console.log(key, keyPath);
    },
  },
}
</script>

<style>
.affix-container{
 margin:0; 
 padding:0
}

#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
}

body { margin: 0; padding:0}
</style>

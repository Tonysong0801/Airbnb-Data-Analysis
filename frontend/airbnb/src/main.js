import { createApp } from 'vue'
import App from './App.vue'

import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'

import axios from 'axios'
import router from './router'

// import "@/plugins/echarts";

axios.defaults.timeout = 300000
axios.defaults.baseURL = 'http://localhost:8000'

const app = createApp(App)

app.use(ElementPlus)
app.use(router, axios)
app.mount('#app')

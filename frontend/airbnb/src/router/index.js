import { createRouter, createWebHistory } from 'vue-router'
import theHome from '../views/Home.vue'
import ReviewAnalysis from '../views/ReviewAnalysis.vue'
import Recommend from '../views/Recommend.vue'
import PredictPrice from '../views/PredictPrice.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: theHome,
  },
  {
    path: '/review_analysis',
    name: 'ReviewAnalysis',
    component: ReviewAnalysis,
  },
  {
    path: '/recommend',
    name: 'theRecommend',
    component: Recommend,
  },
  {
    path: '/predict_price',
    name: 'PredictPrice',
    component: PredictPrice,
  },
]

const router = createRouter({
  history: createWebHistory(process.env.BASE_URL),
  routes
})

export default router

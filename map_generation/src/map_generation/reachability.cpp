#include <map_generation/reachability.h>
#include <stdexcept>
#include <atomic>
#include <chrono>
#include <ctime>
#include <thread>
#include <cmath>
#include <cfloat>
#include <unordered_map>
#ifdef _OPENMP
#include <omp.h>
#endif

namespace reuleaux
{
ReachAbility::ReachAbility(ros::NodeHandle& node, std::string group_name,
                           const std::string& ee_frame, bool check_collision)
  :group_name_(group_name),
  ee_frame_(ee_frame),
  check_collision_(check_collision)
{
  nh_ = node;
  client_ = nh_.serviceClient<moveit_msgs::GetPositionIK>("/compute_ik");

  // Wait for IK service to be available
  ROS_INFO("Waiting for IK service...");
  if (!client_.waitForExistence(ros::Duration(10.0)))
  {
    ROS_ERROR("IK service /compute_ik not available after 10 seconds");
    throw std::runtime_error("IK service not available");
  }
  ROS_INFO("IK service is available");

  group_.reset(new moveit::planning_interface::MoveGroupInterface(group_name_));
  planning_frame_ = group_->getPlanningFrame();

  ROS_INFO("-------------------------------------------------");
  ROS_INFO_STREAM("Current planning frame: " << planning_frame_);
  ROS_INFO("-------------------------------------------------");

  std::string current_ee_frame_;
  current_ee_frame_ = group_->getEndEffectorLink();
  ROS_INFO("-------------------------------------------------");
  ROS_INFO_STREAM("Current end effector frame: " << current_ee_frame_);
  ROS_INFO_STREAM("Attempting to set effector frame: " << ee_frame);
  group_->setEndEffectorLink(ee_frame);
  current_ee_frame_ = group_->getEndEffectorLink();
  ROS_INFO_STREAM("Current end effector frame: " << current_ee_frame_);
  ROS_INFO("-------------------------------------------------");

  robot_model_loader_.reset(new robot_model_loader::RobotModelLoader("robot_description"));
  robot_model_ = robot_model_loader_->getModel();
  if (!robot_model_)
    throw std::runtime_error("Failed to load robot model from robot_description");
  joint_model_group_ = robot_model_->getJointModelGroup(group_name_);
  if (!joint_model_group_)
    throw std::runtime_error("Failed to get joint model group: " + group_name_);

  final_ws_.WsSpheres.clear();
  init_ws_.WsSpheres.clear();

}

 geometry_msgs::PoseStamped ReachAbility::makePoseStamped(const geometry_msgs::Pose& pose_in)
{
  geometry_msgs::PoseStamped pose_st;
  pose_st.header.frame_id = planning_frame_;
  pose_st.pose = pose_in;
  return pose_st;
}

 moveit_msgs::PositionIKRequest ReachAbility::makeServiceRequest(const geometry_msgs::Pose &pose_in)
 {
   moveit_msgs::PositionIKRequest req;
   geometry_msgs::PoseStamped pose_st = makePoseStamped(pose_in);
   req.group_name = group_name_;
   req.ik_link_name = ee_frame_;
   req.avoid_collisions = check_collision_;
   req.timeout.fromSec(0.1);
   req.pose_stamped = pose_st;
   return req;
 }

 void ReachAbility::setInitialWorkspace(const map_generation::WorkSpace &initial_ws)
 {
   init_ws_ = initial_ws;
   reuleaux::getPoseAndSphereSize(init_ws_, sphere_size_, pose_size_);
 }

 void ReachAbility::getFinalWorkspace(map_generation::WorkSpace &final_ws)
 {
   final_ws = final_ws_;
 }

 void ReachAbility::transformTaskpose(const geometry_msgs::Pose &base_pose, const geometry_msgs::Pose &pose_in, geometry_msgs::Pose &pose_out)
 {
   //First get the transform between the new base pose to world (inv)
   Eigen::Affine3d base_pose_tf;
   tf::poseMsgToEigen(base_pose, base_pose_tf);
   Eigen::Affine3d base_pose_to_world_tf = base_pose_tf.inverse();
   //Get the transform between the task pose to world
   Eigen::Affine3d reach_pose_tf;
   tf::poseMsgToEigen(pose_in, reach_pose_tf);
   //transform the task pose to base pose at center. Now we can get Ik for this pose
   tf::poseEigenToMsg(base_pose_to_world_tf * reach_pose_tf, pose_out);
 }

 bool ReachAbility::ik(const moveit_msgs::PositionIKRequest& req, moveit_msgs::RobotState &robot_state)
 {
   srv_.request.ik_request = req;
   if(client_.call(srv_))
   {
     if(srv_.response.error_code.val == 1)
     {
       robot_state = srv_.response.solution;
       return true;
     }
     else
       return false;
    }
   else
   {
     ROS_ERROR("Failed to call IK service");
     return false;
   }
 }

 bool ReachAbility::getIKSolution(const geometry_msgs::Pose &pose, moveit_msgs::RobotState &robot_state)
 {
   ROS_DEBUG("===============================");
   ROS_DEBUG("Requesting IK solution for...");
   ROS_DEBUG("Position: x: %f y: %f z: %f", pose.position.x, pose.position.y, pose.position.z);
   ROS_DEBUG("Orientation: qx: %f qy: %f qz: %f qw: %f", pose.orientation.x, pose.orientation.y, pose.orientation.z, pose.orientation.w);
   moveit_msgs::PositionIKRequest req = makeServiceRequest(pose);
   if(ik(req, robot_state))
     return true;
   else
     return false;
 }

 bool ReachAbility::getIKSolution(const geometry_msgs::Pose &pose, std::vector<double> &joint_solution)
 {
   moveit_msgs::PositionIKRequest req = makeServiceRequest(pose);
   moveit_msgs::RobotState robot_state;
   std::vector<std::string> joint_names;
   if(ik(req, robot_state))
   {
     std::vector<std::string> full_names = robot_state.joint_state.name;
     joint_names = group_->getVariableNames();
     for(size_t i=0;i<joint_names.size();++i)
     {
       auto it = std::find(full_names.begin(), full_names.end(), joint_names[i]);
       if (it == full_names.end())
       {
         ROS_ERROR("Joint '%s' not found in IK solution", joint_names[i].c_str());
         return false;
       }
       joint_solution.push_back(robot_state.joint_state.position[it - full_names.begin()]);
     }
     return true;
   }
   else
     return false;
 }

 bool ReachAbility::getIKSolutionFromTfBase(const geometry_msgs::Pose &base_pose, const geometry_msgs::Pose &pose, moveit_msgs::RobotState &robot_state)
 {
   geometry_msgs::Pose task_pose;
   transformTaskpose(base_pose, pose, task_pose);
   moveit_msgs::PositionIKRequest req = makeServiceRequest(task_pose);
   if(ik(req, robot_state))
     return true;
   else
     return false;
 }

 bool ReachAbility::getIKSolutionFromTfBase(const geometry_msgs::Pose &base_pose, const geometry_msgs::Pose &pose, std::vector<double> &joint_solution)
 {
   geometry_msgs::Pose task_pose;
   transformTaskpose(base_pose, pose, task_pose);
   moveit_msgs::PositionIKRequest req = makeServiceRequest(task_pose);
   moveit_msgs::RobotState robot_state;
   std::vector<std::string> joint_names;
   if(ik(req, robot_state))
   {
     std::vector<std::string> full_names = robot_state.joint_state.name;
     joint_names = group_->getVariableNames();
     for(size_t i=0;i<joint_names.size();++i)
     {
       auto it = std::find(full_names.begin(), full_names.end(), joint_names[i]);
       if (it == full_names.end())
       {
         ROS_ERROR("Joint '%s' not found in IK solution", joint_names[i].c_str());
         return false;
       }
       joint_solution.push_back(robot_state.joint_state.position[it - full_names.begin()]);
     }
     return true;
   }
   else
     return false;

 }

 bool ReachAbility::createReachableWorkspace()
 {
   bool use_flood_fill = false;
   nh_.param<bool>("use_flood_fill", use_flood_fill, false);
   if (use_flood_fill)
   {
     ROS_INFO("Using flood-fill BFS reachability strategy.");
     bool ok = createReachabilityFloodFill(init_ws_);
     if (!ok)
     {
       ROS_WARN("Flood-fill failed. Falling back to exhaustive mode.");
       return createReachability(init_ws_);
     }
     return true;
   }
   return createReachability(init_ws_);
 }

 bool ReachAbility::createReachability(const map_generation::WorkSpace& ws)
 {
   int sp_size = sphere_size_;

   bool use_service_ik = false;
   nh_.param<bool>("use_service_ik", use_service_ik, use_service_ik);

   // Per-sphere result storage: index by sphere so threads write to disjoint entries.
   // With incremental saving, only one batch of sphere_maps is live at a time so
   // peak memory stays bounded regardless of total map size.
   std::vector<reuleaux::MultiMap> sphere_maps(sp_size);
   const auto start_time = std::chrono::steady_clock::now();

   // 20 checkpoints (every 5%) gives ~2 h intervals for a 0.05-resolution map.
   const int batch_size = std::max(1, sp_size / 20);

   auto format_duration = [](double seconds) -> std::string {
     int h = static_cast<int>(seconds) / 3600;
     int m = (static_cast<int>(seconds) % 3600) / 60;
     int s = static_cast<int>(seconds) % 60;
     char buf[32];
     if (h > 0)
       snprintf(buf, sizeof(buf), "%dh %dm %ds", h, m, s);
     else if (m > 0)
       snprintf(buf, sizeof(buf), "%dm %ds", m, s);
     else
       snprintf(buf, sizeof(buf), "%ds", s);
     return std::string(buf);
   };

   auto log_progress = [&](int done) {
     double elapsed = std::chrono::duration<double>(
       std::chrono::steady_clock::now() - start_time).count();
     if (done == sp_size)
     {
       ROS_INFO("Processed %d / %d spheres (100%%) - completed in %s",
                done, sp_size, format_duration(elapsed).c_str());
     }
     else
     {
       double eta = (elapsed / done) * (sp_size - done);
       auto eta_wall = std::chrono::system_clock::now() +
                       std::chrono::duration_cast<std::chrono::system_clock::duration>(
                         std::chrono::duration<double>(eta));
       std::time_t eta_t = std::chrono::system_clock::to_time_t(eta_wall);
       struct tm eta_tm;
       localtime_r(&eta_t, &eta_tm);
       char time_buf[16];
       std::strftime(time_buf, sizeof(time_buf), "%H:%M:%S", &eta_tm);
       ROS_INFO("Processed %d / %d spheres (%.0f%%) - ETA %s",
                done, sp_size, 100.0 * done / sp_size, time_buf);
     }
   };

   // Flush one completed batch: call write_callback_ then free its memory.
   // When no callback is registered sphere_maps entries are kept for the
   // fallback in-memory final_ws_ construction below.
   auto flush_batch = [&](int b_start, int b_end) {
     if (write_callback_)
     {
       write_callback_(sphere_maps, b_start, b_end, pose_size_, sphere_size_);
       for (int i = b_start; i < b_end; ++i)
         reuleaux::MultiMap().swap(sphere_maps[i]);
     }
   };

   if (use_service_ik)
   {
     ROS_INFO("Processing %d spheres via /compute_ik service (serial, batch_size=%d)",
              sp_size, batch_size);

     for (int batch_start = 0; batch_start < sp_size; batch_start += batch_size)
     {
       int batch_end = std::min(batch_start + batch_size, sp_size);
       for (int i = batch_start; i < batch_end; ++i)
       {
         std::vector<double> sp_vec;
         reuleaux::pointToVector(ws.WsSpheres[i].point, sp_vec);
         for (int j = 0; j < static_cast<int>(ws.WsSpheres[i].poses.size()); ++j)
         {
           moveit_msgs::GetPositionIK local_srv;
           local_srv.request.ik_request = makeServiceRequest(ws.WsSpheres[i].poses[j]);
           if (client_.call(local_srv) && local_srv.response.error_code.val == 1)
           {
             std::vector<double> sp_pose;
             reuleaux::poseToVector(ws.WsSpheres[i].poses[j], sp_pose);
             sphere_maps[i].insert(std::make_pair(sp_vec, sp_pose));
           }
         }
       }
       log_progress(batch_end);
       flush_batch(batch_start, batch_end);
     }
   }
   else
   {
     // Determine thread count: default to hardware concurrency, overridable via num_ik_threads ROS param
     int num_threads = static_cast<int>(std::thread::hardware_concurrency());
     if (num_threads <= 0) num_threads = 4;
     nh_.param<int>("num_ik_threads", num_threads, num_threads);
     num_threads = std::min(num_threads, sp_size);

     // Number of IK attempts per pose: on collision, retry with a random seed to exploit redundancy.
     int num_ik_attempts = 5;
     nh_.param<int>("num_ik_attempts", num_ik_attempts, num_ik_attempts);

     ROS_INFO("Processing %d spheres using %d threads (%d IK attempts per pose, batch_size=%d)",
              sp_size, num_threads, num_ik_attempts, batch_size);

     // Load per-thread robot models once, before the batch loop.
     ROS_INFO("Loading per-thread robot models (%d threads) in parallel...", num_threads);
     std::vector<robot_model_loader::RobotModelLoaderPtr> thread_loaders(num_threads);
     std::vector<robot_model::RobotModelConstPtr>         thread_models(num_threads);
     std::vector<const robot_model::JointModelGroup*>     thread_jmgs(num_threads);
     std::vector<robot_state::RobotStatePtr>              thread_states(num_threads);
     std::vector<planning_scene::PlanningScenePtr>        thread_scenes(num_threads);
     std::vector<std::string>                             load_errors(num_threads);

     {
       std::vector<std::thread> load_threads(num_threads);
       for (int t = 0; t < num_threads; ++t)
       {
         load_threads[t] = std::thread(
           [t, &thread_loaders, &thread_models, &thread_jmgs,
            &thread_states, &thread_scenes, &load_errors, this]()
           {
             try
             {
               thread_loaders[t].reset(new robot_model_loader::RobotModelLoader("robot_description"));
               thread_models[t] = thread_loaders[t]->getModel();
               if (!thread_models[t]) { load_errors[t] = "getModel() returned null"; return; }
               thread_jmgs[t] = thread_models[t]->getJointModelGroup(group_name_);
               if (!thread_jmgs[t]) { load_errors[t] = "getJointModelGroup() returned null"; return; }
               thread_states[t].reset(new robot_state::RobotState(thread_models[t]));
               thread_states[t]->setToDefaultValues();
               thread_scenes[t].reset(new planning_scene::PlanningScene(thread_models[t]));
             }
             catch (const std::exception& e) { load_errors[t] = e.what(); }
           });
       }
       for (auto& th : load_threads) th.join();
     }

     for (int t = 0; t < num_threads; ++t)
       if (!load_errors[t].empty())
         throw std::runtime_error("Robot model load failed for thread " +
                                  std::to_string(t) + ": " + load_errors[t]);
     ROS_INFO("Per-thread robot models loaded.");

     // Process sphere batches sequentially; within each batch use parallel IK.
     // After each batch the results are flushed to disk and freed so that
     // sphere_maps never holds more than one batch in memory at a time.
     for (int batch_start = 0; batch_start < sp_size; batch_start += batch_size)
     {
       int batch_end = std::min(batch_start + batch_size, sp_size);

       #pragma omp parallel for num_threads(num_threads) schedule(dynamic)
       for (int i = batch_start; i < batch_end; ++i)
       {
 #ifdef _OPENMP
         int tid = omp_get_thread_num();
 #else
         int tid = 0;
 #endif
         std::vector<double> sp_vec;
         reuleaux::pointToVector(ws.WsSpheres[i].point, sp_vec);

         robot_state::RobotState& state = *thread_states[tid];
         planning_scene::PlanningScene& scene = *thread_scenes[tid];

         robot_state::GroupStateValidityCallbackFn constraint_fn;
         if (check_collision_)
         {
           constraint_fn = [&scene](robot_state::RobotState* rs,
                                    const robot_model::JointModelGroup* jmg,
                                    const double* joint_values)
           {
             rs->setJointGroupPositions(jmg, joint_values);
             rs->update();
             collision_detection::CollisionRequest col_req;
             collision_detection::CollisionResult col_res;
             scene.checkSelfCollision(col_req, col_res, *rs);
             return !col_res.collision;
           };
         }

         for (int j = 0; j < static_cast<int>(ws.WsSpheres[i].poses.size()); ++j)
         {
           const geometry_msgs::Pose& reach_pose = ws.WsSpheres[i].poses[j];

           std::vector<double> warm_seed;
           state.copyJointGroupPositions(thread_jmgs[tid], warm_seed);

           bool found_valid = false;
           for (int attempt = 0; attempt < num_ik_attempts && !found_valid; ++attempt)
           {
             if (attempt > 0)
               state.setToRandomPositions(thread_jmgs[tid]);

             if (state.setFromIK(thread_jmgs[tid], reach_pose, ee_frame_, 0.1, constraint_fn))
             {
               std::vector<double> sp_pose;
               reuleaux::poseToVector(reach_pose, sp_pose);
               sphere_maps[i].insert(std::make_pair(sp_vec, sp_pose));
               found_valid = true;
             }
           }

           if (!found_valid)
             state.setJointGroupPositions(thread_jmgs[tid], warm_seed);
         }
       } // end omp parallel for

       log_progress(batch_end);
       flush_batch(batch_start, batch_end);
     }
   }

   // Save the resolution and orientation count before we free init_ws_ below.
   const double saved_resolution = ws.resolution;
   const int orientations_per_sphere = (!ws.WsSpheres.empty())
     ? static_cast<int>(ws.WsSpheres[0].poses.size()) : 1;

   // Free init_ws_ (no longer needed after the parallel IK pass) to reclaim
   // ~1.5 GB before the memory-intensive final_ws_ construction below.
   { map_generation::WorkSpace empty; std::swap(init_ws_, empty); }

   // Build final_ws_ directly from the per-sphere result maps, freeing each
   // entry as we go.  The old code merged all sphere_maps into a single
   // ws_map first, which duplicated the entire dataset (~5-8 GB extra) and
   // caused an OOM crash on large maps.
   for (int i = 0; i < sp_size; ++i)
   {
     if (sphere_maps[i].empty()) continue;

     map_generation::WsSphere wss;
     const std::vector<double>& sp_coord = sphere_maps[i].begin()->first;
     wss.point.x = sp_coord[0];
     wss.point.y = sp_coord[1];
     wss.point.z = sp_coord[2];
     wss.ri = (orientations_per_sphere > 0)
       ? 100.0 * static_cast<int>(sphere_maps[i].size()) / orientations_per_sphere
       : 0.0;

     for (auto& entry : sphere_maps[i])
     {
       geometry_msgs::Pose pp;
       pp.position.x    = entry.second[0];
       pp.position.y    = entry.second[1];
       pp.position.z    = entry.second[2];
       pp.orientation.x = entry.second[3];
       pp.orientation.y = entry.second[4];
       pp.orientation.z = entry.second[5];
       pp.orientation.w = entry.second[6];
       wss.poses.push_back(pp);
     }
     final_ws_.WsSpheres.push_back(wss);

     // Free this sphere's data immediately to keep peak memory low.
     reuleaux::MultiMap().swap(sphere_maps[i]);
   }
   final_ws_.resolution = saved_resolution;

   return true;
 }

 bool ReachAbility::createReachabilityFloodFill(const map_generation::WorkSpace& ws)
 {
   int sp_size = sphere_size_;
   if (sp_size == 0) return false;

   // Save resolution and orientation count before init_ws_ is freed.
   const double saved_resolution = ws.resolution;
   const int orientations_per_sphere = (!ws.WsSpheres.empty())
     ? static_cast<int>(ws.WsSpheres[0].poses.size()) : 1;

   std::vector<reuleaux::MultiMap> sphere_maps(sp_size);
   const auto start_time = std::chrono::steady_clock::now();
   const int batch_size = std::max(1, sp_size / 20);

   auto format_duration = [](double seconds) -> std::string {
     int h = static_cast<int>(seconds) / 3600;
     int m = (static_cast<int>(seconds) % 3600) / 60;
     int s = static_cast<int>(seconds) % 60;
     char buf[32];
     if (h > 0)
       snprintf(buf, sizeof(buf), "%dh %dm %ds", h, m, s);
     else if (m > 0)
       snprintf(buf, sizeof(buf), "%dm %ds", m, s);
     else
       snprintf(buf, sizeof(buf), "%ds", s);
     return std::string(buf);
   };

   auto log_progress = [&](int done) {
     double elapsed = std::chrono::duration<double>(
       std::chrono::steady_clock::now() - start_time).count();
     ROS_INFO("[FloodFill] Processed %d / %d spheres (%.1f%%) - elapsed %s",
              done, sp_size, 100.0 * done / sp_size,
              format_duration(elapsed).c_str());
   };

   // --- Thread setup (same as createReachability) ---
   int num_threads = static_cast<int>(std::thread::hardware_concurrency());
   if (num_threads <= 0) num_threads = 4;
   nh_.param<int>("num_ik_threads", num_threads, num_threads);
   num_threads = std::min(num_threads, sp_size);

   int num_ik_attempts = 5;
   nh_.param<int>("num_ik_attempts", num_ik_attempts, num_ik_attempts);

   ROS_INFO("[FloodFill] Using %d threads, %d IK attempts per pose",
            num_threads, num_ik_attempts);

   std::vector<robot_model_loader::RobotModelLoaderPtr> thread_loaders(num_threads);
   std::vector<robot_model::RobotModelConstPtr>         thread_models(num_threads);
   std::vector<const robot_model::JointModelGroup*>     thread_jmgs(num_threads);
   std::vector<robot_state::RobotStatePtr>              thread_states(num_threads);
   std::vector<planning_scene::PlanningScenePtr>        thread_scenes(num_threads);
   std::vector<std::string>                             load_errors(num_threads);

   {
     std::vector<std::thread> load_threads(num_threads);
     for (int t = 0; t < num_threads; ++t)
     {
       load_threads[t] = std::thread(
         [t, &thread_loaders, &thread_models, &thread_jmgs,
          &thread_states, &thread_scenes, &load_errors, this]()
         {
           try
           {
             thread_loaders[t].reset(new robot_model_loader::RobotModelLoader("robot_description"));
             thread_models[t] = thread_loaders[t]->getModel();
             if (!thread_models[t]) { load_errors[t] = "getModel() returned null"; return; }
             thread_jmgs[t] = thread_models[t]->getJointModelGroup(group_name_);
             if (!thread_jmgs[t]) { load_errors[t] = "getJointModelGroup() returned null"; return; }
             thread_states[t].reset(new robot_state::RobotState(thread_models[t]));
             thread_states[t]->setToDefaultValues();
             thread_scenes[t].reset(new planning_scene::PlanningScene(thread_models[t]));
           }
           catch (const std::exception& e) { load_errors[t] = e.what(); }
         });
     }
     for (auto& th : load_threads) th.join();
   }

   for (int t = 0; t < num_threads; ++t)
     if (!load_errors[t].empty())
       throw std::runtime_error("Robot model load failed for thread " +
                                std::to_string(t) + ": " + load_errors[t]);
   ROS_INFO("[FloodFill] Per-thread robot models loaded.");

   // --- Grid-index lookup ---
   double res = ws.resolution;
   if (res <= 0.0)
   {
     nh_.param<double>("resolution", res, 0.05);
     ROS_WARN("[FloodFill] ws.resolution was %.6f (unset?); using ROS param resolution=%.4f",
              (double)ws.resolution, res);
   }
   double x_min = DBL_MAX, y_min = DBL_MAX, z_min = DBL_MAX;
   double y_max = -DBL_MAX;
   for (int i = 0; i < sp_size; ++i)
   {
     x_min = std::min(x_min, (double)ws.WsSpheres[i].point.x);
     y_min = std::min(y_min, (double)ws.WsSpheres[i].point.y);
     z_min = std::min(z_min, (double)ws.WsSpheres[i].point.z);
     y_max = std::max(y_max, (double)ws.WsSpheres[i].point.y);
   }

   auto to_ix = [&](double x) { return (int)std::llround((x - x_min) / res); };
   auto to_iy = [&](double y) { return (int)std::llround((y - y_min) / res); };
   auto to_iz = [&](double z) { return (int)std::llround((z - z_min) / res); };

   // Packed 64-bit key; stride of 1024 supports up to ~102 m workspace at res=0.1 m.
   const int64_t SY = 1024, SZ = 1024 * 1024;
   auto make_key = [&](int ix, int iy, int iz) -> int64_t {
     return (int64_t)ix + (int64_t)iy * SY + (int64_t)iz * SZ;
   };

   std::vector<int> sph_ix(sp_size), sph_iy(sp_size), sph_iz(sp_size);
   std::unordered_map<int64_t, int> grid_index;
   grid_index.reserve(sp_size);
   for (int i = 0; i < sp_size; ++i)
   {
     sph_ix[i] = to_ix(ws.WsSpheres[i].point.x);
     sph_iy[i] = to_iy(ws.WsSpheres[i].point.y);
     sph_iz[i] = to_iz(ws.WsSpheres[i].point.z);
     grid_index[make_key(sph_ix[i], sph_iy[i], sph_iz[i])] = i;
   }

   // --- Seed selection: XZ mid-slice (fixed Y) ---
   // Use integer grid Y-indices (sph_iy) so there is no floating-point
   // rounding between grid rows.  The raw (y_min+y_max)/2 formula can fall
   // exactly between two adjacent rows, causing 0 seed spheres.
   int iy_min = sph_iy[0], iy_max = sph_iy[0];
   for (int i = 0; i < sp_size; ++i)
   {
     iy_min = std::min(iy_min, sph_iy[i]);
     iy_max = std::max(iy_max, sph_iy[i]);
   }
   const int center_iy = (iy_min + iy_max) / 2;

   auto collect_iy_slice = [&](int iy_target) -> std::vector<int> {
     std::vector<int> slice;
     for (int i = 0; i < sp_size; ++i)
       if (sph_iy[i] == iy_target) slice.push_back(i);
     return slice;
   };

   std::vector<bool> visited(sp_size, false);
   std::vector<int> frontier = collect_iy_slice(center_iy);
   for (int idx : frontier) visited[idx] = true;

   ROS_INFO("[FloodFill] Seed XZ-slice: center_iy=%d (y~=%.3f), %zu seed spheres",
            center_iy, y_min + center_iy * res, frontier.size());

   // 6-connected neighbour offsets
   const int DX[6] = {+1,-1, 0, 0, 0, 0};
   const int DY[6] = { 0, 0,+1,-1, 0, 0};
   const int DZ[6] = { 0, 0, 0, 0,+1,-1};

   // Helper: run IK for a single sphere index on thread 0 (serial use only).
   auto run_ik_serial = [&](int i)
   {
     std::vector<double> sp_vec;
     reuleaux::pointToVector(ws.WsSpheres[i].point, sp_vec);
     robot_state::RobotState& state = *thread_states[0];
     planning_scene::PlanningScene& scene = *thread_scenes[0];

     robot_state::GroupStateValidityCallbackFn constraint_fn;
     if (check_collision_)
     {
       constraint_fn = [&scene](robot_state::RobotState* rs,
                                const robot_model::JointModelGroup* jmg,
                                const double* joint_values)
       {
         rs->setJointGroupPositions(jmg, joint_values);
         rs->update();
         collision_detection::CollisionRequest col_req;
         collision_detection::CollisionResult col_res;
         scene.checkSelfCollision(col_req, col_res, *rs);
         return !col_res.collision;
       };
     }

     for (int j = 0; j < static_cast<int>(ws.WsSpheres[i].poses.size()); ++j)
     {
       const geometry_msgs::Pose& reach_pose = ws.WsSpheres[i].poses[j];
       std::vector<double> warm_seed;
       state.copyJointGroupPositions(thread_jmgs[0], warm_seed);
       bool found_valid = false;
       for (int attempt = 0; attempt < num_ik_attempts && !found_valid; ++attempt)
       {
         if (attempt > 0) state.setToRandomPositions(thread_jmgs[0]);
         if (state.setFromIK(thread_jmgs[0], reach_pose, ee_frame_, 0.1, constraint_fn))
         {
           std::vector<double> sp_pose;
           reuleaux::poseToVector(reach_pose, sp_pose);
           sphere_maps[i].insert(std::make_pair(sp_vec, sp_pose));
           found_valid = true;
         }
       }
       if (!found_valid)
         state.setJointGroupPositions(thread_jmgs[0], warm_seed);
     }
   };

   // Flush accumulator
   int total_processed = 0;
   std::vector<int> pending_flush;
   pending_flush.reserve(batch_size);

   // Flush by iterating 0..sp_size and skipping empties (callback skips them).
   auto flush_processed = [&](std::vector<int>& pending) {
     if (!write_callback_ || pending.empty()) return;
     write_callback_(sphere_maps, 0, sp_size, pose_size_, sphere_size_);
     for (int idx : pending)
       reuleaux::MultiMap().swap(sphere_maps[idx]);
     pending.clear();
   };

   bool seed_fallback_done = false;

   // --- BFS loop ---
   while (!frontier.empty())
   {
     int fsz = (int)frontier.size();

     // Parallel IK over current frontier
     #pragma omp parallel for num_threads(num_threads) schedule(dynamic)
     for (int fi = 0; fi < fsz; ++fi)
     {
       int i = frontier[fi];
 #ifdef _OPENMP
       int tid = omp_get_thread_num();
 #else
       int tid = 0;
 #endif
       std::vector<double> sp_vec;
       reuleaux::pointToVector(ws.WsSpheres[i].point, sp_vec);

       robot_state::RobotState& state = *thread_states[tid];
       planning_scene::PlanningScene& scene = *thread_scenes[tid];

       robot_state::GroupStateValidityCallbackFn constraint_fn;
       if (check_collision_)
       {
         constraint_fn = [&scene](robot_state::RobotState* rs,
                                  const robot_model::JointModelGroup* jmg,
                                  const double* joint_values)
         {
           rs->setJointGroupPositions(jmg, joint_values);
           rs->update();
           collision_detection::CollisionRequest col_req;
           collision_detection::CollisionResult col_res;
           scene.checkSelfCollision(col_req, col_res, *rs);
           return !col_res.collision;
         };
       }

       for (int j = 0; j < static_cast<int>(ws.WsSpheres[i].poses.size()); ++j)
       {
         const geometry_msgs::Pose& reach_pose = ws.WsSpheres[i].poses[j];
         std::vector<double> warm_seed;
         state.copyJointGroupPositions(thread_jmgs[tid], warm_seed);
         bool found_valid = false;
         for (int attempt = 0; attempt < num_ik_attempts && !found_valid; ++attempt)
         {
           if (attempt > 0)
             state.setToRandomPositions(thread_jmgs[tid]);
           if (state.setFromIK(thread_jmgs[tid], reach_pose, ee_frame_, 0.1, constraint_fn))
           {
             std::vector<double> sp_pose;
             reuleaux::poseToVector(reach_pose, sp_pose);
             sphere_maps[i].insert(std::make_pair(sp_vec, sp_pose));
             found_valid = true;
           }
         }
         if (!found_valid)
           state.setJointGroupPositions(thread_jmgs[tid], warm_seed);
       }
     } // end omp parallel for

     total_processed += fsz;

     // --- Seed fallback (first iteration only) ---
     if (!seed_fallback_done)
     {
       bool any_reachable = false;
       for (int idx : frontier)
         if (!sphere_maps[idx].empty()) { any_reachable = true; break; }

       if (!any_reachable)
       {
         // Cover the full Y range from center to either edge.
         const int max_y_steps = std::max(center_iy - iy_min, iy_max - center_iy) + 1;
         for (int step = 1; step <= max_y_steps && !any_reachable; ++step)
         {
           for (int sign : {-1, +1})
           {
             int iy_try = center_iy + sign * step;
             if (iy_try < iy_min || iy_try > iy_max) continue;
             auto extra = collect_iy_slice(iy_try);
             std::vector<int> unvisited;
             for (int idx : extra)
               if (!visited[idx]) { visited[idx] = true; unvisited.push_back(idx); }
             if (unvisited.empty()) continue;

             for (int idx : unvisited)
               run_ik_serial(idx);

             for (int idx : unvisited)
               if (!sphere_maps[idx].empty()) { any_reachable = true; break; }

             frontier.insert(frontier.end(), unvisited.begin(), unvisited.end());
             total_processed += (int)unvisited.size();

             if (any_reachable) break;
             ROS_DEBUG("[FloodFill] Y-slice iy=%d had no reachable spheres, trying next.",
                       iy_try);
           }
         }

         if (!any_reachable)
         {
           ROS_ERROR("[FloodFill] No reachable spheres found across all Y-slices. "
                     "Falling back to exhaustive.");
           return false;
         }
         ROS_INFO("[FloodFill] Found reachable spheres after Y-slice expansion.");
         // Update fsz so neighbour collection includes fallback spheres.
         fsz = (int)frontier.size();
       }
       seed_fallback_done = true;
     }

     // --- Collect next frontier (serial) ---
     std::vector<int> next_frontier;
     for (int fi = 0; fi < fsz; ++fi)
     {
       int i = frontier[fi];
       if (sphere_maps[i].empty()) continue;  // unreachable: do not expand
       for (int d = 0; d < 6; ++d)
       {
         int64_t nk = make_key(sph_ix[i]+DX[d], sph_iy[i]+DY[d], sph_iz[i]+DZ[d]);
         auto it = grid_index.find(nk);
         if (it == grid_index.end()) continue;
         int ni = it->second;
         if (visited[ni]) continue;
         visited[ni] = true;
         next_frontier.push_back(ni);
       }
     }

     // --- Flush accumulator ---
     for (int idx : frontier) pending_flush.push_back(idx);
     if ((int)pending_flush.size() >= batch_size || next_frontier.empty())
     {
       log_progress(total_processed);
       flush_processed(pending_flush);
     }

     frontier = std::move(next_frontier);
   }

   ROS_INFO("[FloodFill] BFS done. Evaluated %d/%d spheres (%.1f%%)",
            total_processed, sp_size, 100.0 * total_processed / sp_size);

   // Free init_ws_ before building final_ws_ to reclaim memory.
   { map_generation::WorkSpace empty; std::swap(init_ws_, empty); }

   // Build final_ws_ from sphere_maps (used when no write_callback_ is set).
   for (int i = 0; i < sp_size; ++i)
   {
     if (sphere_maps[i].empty()) continue;

     map_generation::WsSphere wss;
     const std::vector<double>& sp_coord = sphere_maps[i].begin()->first;
     wss.point.x = sp_coord[0];
     wss.point.y = sp_coord[1];
     wss.point.z = sp_coord[2];
     wss.ri = (orientations_per_sphere > 0)
       ? 100.0 * static_cast<int>(sphere_maps[i].size()) / orientations_per_sphere
       : 0.0;

     for (auto& entry : sphere_maps[i])
     {
       geometry_msgs::Pose pp;
       pp.position.x    = entry.second[0];
       pp.position.y    = entry.second[1];
       pp.position.z    = entry.second[2];
       pp.orientation.x = entry.second[3];
       pp.orientation.y = entry.second[4];
       pp.orientation.z = entry.second[5];
       pp.orientation.w = entry.second[6];
       wss.poses.push_back(pp);
     }
     final_ws_.WsSpheres.push_back(wss);
     reuleaux::MultiMap().swap(sphere_maps[i]);
   }
   final_ws_.resolution = saved_resolution;

   return true;
 }
}

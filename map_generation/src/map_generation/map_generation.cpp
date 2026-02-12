#include <map_generation/map_generation.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <memory>
#include <sys/statvfs.h>


namespace reuleaux
{
mapGeneration::mapGeneration(ros::NodeHandle& node, const std::string &group_name,
                            const std::string& ee_frame,
                            const std::string &path, const std::string &filename,
                            const std::string& pkg_name, const double &resolution,
                            const double &radius, bool check_collision)
{
  nh_ = node;
  group_name_ = group_name;
  ee_frame_ = ee_frame;
  path_ = path;
  filename_ = filename;
  pkg_name_=pkg_name;
  resolution_ = resolution;
  radius_ = radius;
  check_collision_= check_collision;
  group_.reset(new moveit::planning_interface::MoveGroupInterface(group_name_));

  std::string current_planning_frame_;
  current_planning_frame_ = group_->getPlanningFrame();
  ROS_INFO("-------------------------------------------------");
  ROS_INFO_STREAM("Current planning frame: " << current_planning_frame_);
  ROS_INFO("-------------------------------------------------");

  std::string current_ee_frame_;
  current_ee_frame_ = group_->getEndEffectorLink();
  ROS_INFO("-------------------------------------------------");
  ROS_INFO_STREAM("Current end effector frame: " << current_ee_frame_);
  ROS_INFO_STREAM("Attempting to set effector frame: " << ee_frame_);
  group_->setEndEffectorLink(ee_frame_);
  current_ee_frame_ = group_->getEndEffectorLink();
  ROS_INFO_STREAM("Current end effector frame: " << current_ee_frame_);
  ROS_INFO("-------------------------------------------------");

  init_ws_.WsSpheres.clear();
  filtered_ws_.WsSpheres.clear();
}

void mapGeneration::discretizeWorkspace(geometry_msgs::Pose& pose)
{
  ROS_INFO("Discretizing workspace with resolution %f and radius %f", resolution_, radius_);
  std::unique_ptr<reuleaux::Discretization> disc(new reuleaux::Discretization(pose, resolution_, radius_));
  disc->discretize();
  disc->getInitialWorkspace(init_ws_);
  reuleaux::getPoseAndSphereSize(init_ws_, init_sp_size_, init_pose_size_);
  ROS_INFO("Initial workspace has %d spheres and %d poses", init_sp_size_, init_pose_size_);
}

void mapGeneration::filterWorkspace()
{
  // Build the output filename up front so we can open the HDF5 file for
  // streaming writes before the long IK computation begins.
  std::string out_filename = (filename_ == "default")
    ? reuleaux::createName(pkg_name_, group_name_, resolution_)
    : filename_;
  std::string fullpath = path_ + out_filename;

  // Open the output file now (creates it on disk).  If the process is killed
  // part-way through, the partial file will be present and already flushed up
  // to the last completed checkpoint (~5% interval).
  auto h5 = std::make_shared<reuleaux::Hdf5Dataset>(fullpath);
  h5->openForWrite();
  saved_incrementally_ = true;
  saved_filename_ = out_filename;

  // Capture stats needed to compute per-sphere RI inside the callback.
  const int captured_pose_size   = init_pose_size_;
  const int captured_sphere_size = init_sp_size_;

  std::unique_ptr<reuleaux::ReachAbility> reach(
    new reuleaux::ReachAbility(nh_, group_name_, ee_frame_, check_collision_));
  reach->setInitialWorkspace(init_ws_);
  // init_ws_ has been copied into reach; free the original immediately to
  // reduce peak memory during the long IK computation that follows.
  { map_generation::WorkSpace empty; std::swap(init_ws_, empty); }

  // Register the batch-save callback.  After each ~5% batch the results are
  // converted to HDF5 format, appended to disk, and freed from RAM.
  const float avg_poses = (captured_sphere_size > 0)
    ? float(captured_pose_size) / float(captured_sphere_size) : 1.0f;

  reach->setBatchSaveCallback(
    [h5, avg_poses, this]
    (const std::vector<reuleaux::MultiMap>& sphere_maps,
     int b_start, int b_end, int /*pose_size*/, int /*sphere_size*/)
    {
      reuleaux::VecVecDouble pose_reach;
      reuleaux::VecVecDouble spheres;
      reuleaux::VecDouble    ri;

      for (int i = b_start; i < b_end; ++i)
      {
        if (sphere_maps[i].empty()) continue;
        const std::vector<double>& sp_coord = sphere_maps[i].begin()->first;
        float d = float(sphere_maps[i].size()) / avg_poses * 100.0f;
        spheres.push_back(sp_coord);
        ri.push_back(double(d));
        final_sp_size_++;
        for (const auto& entry : sphere_maps[i])
        {
          std::vector<double> row(10);
          row[0] = sp_coord[0]; row[1] = sp_coord[1]; row[2] = sp_coord[2];
          for (int j = 0; j < 7; ++j) row[3 + j] = entry.second[j];
          pose_reach.push_back(row);
          final_pose_size_++;
        }
      }
      h5->appendSphereBatch(pose_reach, spheres, ri);
    });

  final_sp_size_   = 0;
  final_pose_size_ = 0;
  reach->createReachableWorkspace();
  h5->finalizeWrite(static_cast<float>(resolution_));
}

void mapGeneration::saveWorkspace()
{
  if (saved_incrementally_)
  {
    // Data was already streamed to disk during filterWorkspace(); nothing to do.
    ROS_INFO("%s saved to %s (streamed incrementally)", saved_filename_.c_str(), path_.c_str());
    return;
  }

  // Fallback: write the in-memory filtered_ws_ in one shot (used only when
  // filterWorkspace was not called with streaming, e.g. in unit tests).
  std::string name;
  std::string filename;
  if (filename_ == "default")
    filename = reuleaux::createName(pkg_name_, group_name_, resolution_);
  else
    filename = filename_;
  name = path_ + filename;

  std::unique_ptr<reuleaux::Hdf5Dataset> h5(new reuleaux::Hdf5Dataset(name));
  h5->save(filtered_ws_);
  ROS_INFO("%s saved to %s", filename.c_str(), path_.c_str());
}

void mapGeneration::getArmPose(geometry_msgs::Pose& arm_pose)
{
  std::vector<std::string> link_names = group_->getLinkNames();
  std::string first_link = link_names[0];
  moveit::core::RobotModelConstPtr robot_model = group_->getRobotModel();
  moveit::core::RobotStatePtr robot_state(new moveit::core::RobotState(robot_model));
  Eigen::Affine3d tf_root_to_first_link = robot_state->getGlobalLinkTransform(first_link);
  tf::poseEigenToMsg(tf_root_to_first_link, arm_pose);
}
  
void mapGeneration::generate()
{
  ros::Time startit = ros::Time::now();
  getArmPose(arm_pose_);
  discretizeWorkspace(arm_pose_);
  double dif2 = ros::Duration( ros::Time::now() - startit).toSec();
  filterWorkspace();
  double dif3 = ros::Duration( ros::Time::now() - startit).toSec();

  saveWorkspace();
  ROS_INFO("Time for discretizing workspace %.2lf seconds.", dif2);
  ROS_INFO("Center of workspace   x:%f, y:%f, z:%f", arm_pose_.position.x, arm_pose_.position.y, arm_pose_.position.z);
  ROS_INFO("Time for creating reachable workspace is %.2lf seconds.", dif3);
  ROS_INFO("Initial workspace has %d spheres and %d poses", init_sp_size_, init_pose_size_);
  ROS_INFO("Final workspace has %d spheres and %d poses", final_sp_size_, final_pose_size_);
  ROS_INFO("Completed");
}

}

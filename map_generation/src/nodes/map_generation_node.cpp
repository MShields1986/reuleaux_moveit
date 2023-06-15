#include <ros/ros.h>
#include <map_generation/map_generation.h>



int main(int argc, char **argv)
{
  ros::init(argc, argv, "map_generation_node");
  ros::NodeHandle nh_("~");

  std::string group_name_, ee_frame_, path_, filename_, pkg_name_;
  bool check_collision_;
  double resolution_, radius_;

  if (!nh_.getParam("group_name", group_name_))
  {
    ROS_ERROR("Failed to get required parameter 'group_name'");
    return 1;
  }

  if (!nh_.getParam("ee_frame", ee_frame_))
  {
    ROS_ERROR("Failed to get required parameter 'ee_frame'");
    return 1;
  }

  if (!nh_.getParam("resolution", resolution_))
  {
    ROS_WARN("Parameter 'resolution' not set, using default value 0.08");
    resolution_ = 0.08;
  }

  if (!nh_.getParam("radius", radius_))
  {
    ROS_WARN("Parameter 'radius' not set, using default value 1.0");
    radius_ = 1.0;
  }

  if (!nh_.getParam("check_collision", check_collision_))
  {
    ROS_WARN("Parameter 'check_collision' not set, using default value true");
    check_collision_ = true;
  }

  if (!nh_.getParam("path", path_))
  {
    ROS_ERROR("Failed to get required parameter 'path'");
    return 1;
  }

  if (!nh_.getParam("filename", filename_))
  {
    ROS_WARN("Parameter 'filename' not set, using default value 'default'");
    filename_ = "default";
  }

  if (!nh_.getParam("pkg_name", pkg_name_))
  {
    ROS_ERROR("Failed to get required parameter 'pkg_name'");
    return 1;
  }

  reuleaux::mapGeneration mg(nh_, group_name_, ee_frame_, path_, filename_, pkg_name_,
                             resolution_, radius_, check_collision_);
  mg.generate();

  ros::spin();
  return 0;
}

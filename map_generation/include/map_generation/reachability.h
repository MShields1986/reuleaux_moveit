#ifndef REACHABILITY_H
#define REACHABILITY_H
#include <functional>
#include <ros/ros.h>
#include <map_generation/WorkSpace.h>
#include <map_generation/utility.h>
#include <Eigen/Eigen>
#include <eigen_conversions/eigen_msg.h>
#include <moveit_msgs/GetPositionIK.h>
#include <moveit_msgs/PositionIKRequest.h>
#include <moveit/move_group_interface/move_group_interface.h>
#include <moveit/robot_model_loader/robot_model_loader.h>
#include <moveit/planning_scene/planning_scene.h>
#include <moveit/robot_state/robot_state.h>

namespace reuleaux
{

// Callback invoked after each batch of spheres completes.
// Args: sphere_maps (read-only), batch_start index, batch_end index (exclusive),
//       pose_size (total initial poses), sphere_size (total initial spheres).
using BatchCallback = std::function<void(const std::vector<MultiMap>&, int, int, int, int)>;

class ReachAbility
{
public:
  ReachAbility(ros::NodeHandle& node, std::string group_name,
               const std::string& ee_frame,
               bool check_collision);

  void setInitialWorkspace(const map_generation::WorkSpace& initial_ws);
  void getFinalWorkspace(map_generation::WorkSpace& final_ws);
  bool getIKSolution(const geometry_msgs::Pose& pose, moveit_msgs::RobotState& robot_state);
  bool getIKSolution(const geometry_msgs::Pose &pose, std::vector<double>& joint_solution);
  bool getIKSolutionFromTfBase(const geometry_msgs::Pose& base_pose,
                               const geometry_msgs::Pose& pose, moveit_msgs::RobotState& robot_state);
  bool getIKSolutionFromTfBase(const geometry_msgs::Pose &base_pose,
                               const geometry_msgs::Pose &pose, std::vector<double>& joint_solution);
  bool createReachableWorkspace();

  // Register a callback that will be called after each sphere batch completes.
  // When set, sphere data is written via the callback and freed immediately so
  // that peak RAM stays bounded to one batch at a time.
  void setBatchSaveCallback(BatchCallback cb) { write_callback_ = std::move(cb); }


private:
  std::string group_name_;
  std::string ee_frame_;
  bool check_collision_;
  geometry_msgs::PoseStamped makePoseStamped(const geometry_msgs::Pose& pose_in);
  moveit_msgs::PositionIKRequest makeServiceRequest(const geometry_msgs::Pose &pose_in);
  bool ik(const moveit_msgs::PositionIKRequest& req, moveit_msgs::RobotState& robot_state);
  void transformTaskpose(const geometry_msgs::Pose& base_pose, const geometry_msgs::Pose& pose_in, geometry_msgs::Pose& pose_out);
  bool createReachability(const map_generation::WorkSpace& ws);
  bool createReachabilityFloodFill(const map_generation::WorkSpace& ws);


  boost::scoped_ptr<moveit::planning_interface::MoveGroupInterface> group_;
  std::string planning_frame_;
  moveit_msgs::GetPositionIK srv_;

  ros::NodeHandle nh_;
  ros::ServiceClient client_;
  map_generation::WorkSpace init_ws_;
  map_generation::WorkSpace final_ws_;
  int pose_size_;
  int sphere_size_;

  robot_model_loader::RobotModelLoaderPtr robot_model_loader_;
  robot_model::RobotModelConstPtr robot_model_;
  const robot_model::JointModelGroup* joint_model_group_;

  BatchCallback write_callback_;

};

}//end namespace reuleaux

#endif // REACHABILITY_H

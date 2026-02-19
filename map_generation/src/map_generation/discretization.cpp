#include <map_generation/discretization.h>
#include <memory>


namespace reuleaux
{
Discretization::Discretization()
{
  center_ = octomap::point3d(0,0,0);
  resolution_ = 0.08;
  radius_ = 1.0;
  max_depth_ = 16;
  centers_.resize(0);
  poses_.resize(0);
}

Discretization::Discretization(geometry_msgs::Pose pose, double resolution, double radius ):
  resolution_(resolution), radius_(radius)
{
  center_=octomap::point3d(pose.position.x, pose.position.y, pose.position.z);
  max_depth_ = 16;
  centers_.resize(0);
  poses_.resize(0);
 }

std::unique_ptr<octomap::OcTree> Discretization::generateBoxTree(const octomap::point3d &origin, const double resolution, const double diameter)
{
  // TODO: diameter arg here actually gets passed a variable called radius_ on line 143...needs checking

  std::unique_ptr<octomap::OcTree> tree(new octomap::OcTree(float(resolution)/2));
  // Use integer loop counters so each coordinate is computed as i*resolution
  // (a single double multiply then cast to float) rather than accumulated
  // float32 additions.  Accumulated float32 arithmetic introduces errors
  // > half a voxel (~0.006 m at res=0.025) after ~80 steps, causing some grid
  // points to snap into an adjacent OctoMap cell and creating sparse ghost layers.
  const int n_xy = static_cast<int>(std::round(2.0 * diameter / resolution));
  const int n_z  = static_cast<int>(std::round((origin.z() + diameter) / resolution));
  for (int ix = 0; ix <= n_xy; ++ix)
  {
    const float x = static_cast<float>(origin.x() - diameter + ix * resolution);
    for (int iy = 0; iy <= n_xy; ++iy)
    {
      const float y = static_cast<float>(origin.y() - diameter + iy * resolution);
      for (int iz = 0; iz <= n_z; ++iz)
      {
        const float z = static_cast<float>(iz * resolution);
        tree->updateNode(octomap::point3d(x, y, z), true);
      }
    }
  }
  return tree;
}

void Discretization::createCenters(const std::unique_ptr<octomap::OcTree>& tree, std::vector<geometry_msgs::Point> &centers)
{
  int sphere_count = 0;
  for(octomap::OcTree::leaf_iterator it = tree->begin_leafs(max_depth_),end = tree->end_leafs(); it!=end;++it)
    sphere_count++;
  centers.reserve(sphere_count);
  for(octomap::OcTree::leaf_iterator it = tree->begin_leafs(max_depth_),end = tree->end_leafs(); it!=end;++it)
  {
    geometry_msgs::Point point;
    point.x = (it.getCoordinate()).x();
    point.y = (it.getCoordinate()).y();
    point.z = (it.getCoordinate()).z();
    centers.push_back(point);
  }
}

int Discretization::getNumOfSpheres()
{
  return centers_.size();
}

void Discretization::createPosesOnSphere(const geometry_msgs::Point &center, const double r, std::vector<geometry_msgs::Pose> &poses)
{
  const double DELTA = M_PI/5.;
  const unsigned MAX_INDEX  (2 * 5 *5);
  static std::vector<geometry_msgs::Point> position_vector(MAX_INDEX);
  static std::vector<tf::Quaternion> quaternion(MAX_INDEX);
  static bool initialized = false;
  if(!initialized)
  {
    initialized = true;
    unsigned index = 0;
    for(double phi = 0; phi<2*M_PI; phi+=DELTA)//Azimuth[0,2PI]
    {
      for(double theta = 0;theta<M_PI;theta +=DELTA) //Elevation[0,2PI]
      {
        position_vector[index].x = cos(phi)*sin(theta);
        position_vector[index].y = sin(phi)*sin(theta);
        position_vector[index].z = cos(theta);
        tf::Quaternion quat;
        quat.setRPY(0, ((M_PI/2)+theta), phi);
        quat.normalize();
        quaternion[index] = quat;
        index++;
      }
    }
  }
  poses.reserve(MAX_INDEX);
  poses.clear();
  geometry_msgs::Pose pose;
  for(int i=0;i<MAX_INDEX;++i)
  {
    pose.position.x = r * position_vector[i].x + center.x;
    pose.position.y = r * position_vector[i].y + center.y;
    pose.position.z = r * position_vector[i].z + center.z;
    pose.orientation.x = quaternion[i].x();
    pose.orientation.y = quaternion[i].y();
    pose.orientation.z = quaternion[i].z();
    pose.orientation.w = quaternion[i].w();
    poses.push_back(pose);
  }
}

void Discretization::createPoses(const std::vector<geometry_msgs::Point> &centers, std::vector<geometry_msgs::Pose> &poses)
{
  poses.reserve(centers.size()*50);
  for(int i=0;i<centers.size();++i)
  {
    map_generation::WsSphere wsSphere;
    wsSphere.point = centers[i];
    static std::vector<geometry_msgs::Pose>  pose;
    createPosesOnSphere(centers[i], resolution_, pose);
    for(int j=0;j<pose.size();++j)
    {
      poses.push_back(pose[j]);
      wsSphere.poses.push_back(pose[j]);
     }
    ws_.WsSpheres.push_back(wsSphere);
  }
}

int Discretization::getNumOfPoses()
{
  return poses_.size();
}

void Discretization::getInitialWorkspace(map_generation::WorkSpace& ws)
{
  ws_.resolution = static_cast<float>(resolution_);
  ws = ws_;
}

void Discretization::discretize()
{
  // Compute sphere centres directly using integer loop counters and double
  // arithmetic, bypassing the OcTree intermediate step.
  //
  // The previous approach inserted grid points (at multiples of resolution)
  // into an OcTree with cell size resolution/2.  Every such point falls
  // EXACTLY on an OcTree cell boundary, so whether it snaps to key 2k or
  // 2k-1 depends on float rounding in the key computation.  For example at
  // resolution=0.1: float(0.5)/(float(0.1)/2) ≈ 9.9999998 → key 9 instead
  // of 10, skipping that z-layer entirely and creating visible banding.
  // Skipping the OcTree eliminates all snapping ambiguity.
  centers_.resize(0);
  const int n_xy = static_cast<int>(std::round(2.0 * radius_ / resolution_));
  const int n_z  = static_cast<int>(std::round((static_cast<double>(center_.z()) + radius_) / resolution_));
  centers_.reserve((n_xy + 1) * (n_xy + 1) * (n_z + 1));
  for (int ix = 0; ix <= n_xy; ++ix)
  {
    for (int iy = 0; iy <= n_xy; ++iy)
    {
      for (int iz = 0; iz <= n_z; ++iz)
      {
        geometry_msgs::Point p;
        // Cast to float so that sphere_dataset (float64) and poses_dataset
        // (float32) hold the same value when the Reuleaux loader reads both
        // back as double and compares them with exact equality.
        p.x = static_cast<float>(static_cast<double>(center_.x()) - radius_ + ix * resolution_);
        p.y = static_cast<float>(static_cast<double>(center_.y()) - radius_ + iy * resolution_);
        p.z = static_cast<float>(iz * resolution_);
        centers_.push_back(p);
      }
    }
  }
  createPoses(centers_, poses_);
}

void Discretization::getCenters(std::vector<geometry_msgs::Point> &points)
{
  points = centers_;
}

} //end namespace reuleaux

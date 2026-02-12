#include <ros/ros.h>
#include <map_generation/map_generation.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

// ---------------------------------------------------------------------------
// Helpers: pre-flight checks printed once at startup for every user.
// ---------------------------------------------------------------------------

static unsigned long long availableRamBytes()
{
  long pages     = sysconf(_SC_AVPHYS_PAGES);
  long page_size = sysconf(_SC_PAGESIZE);
  if (pages < 0 || page_size < 0) return 0;
  return static_cast<unsigned long long>(pages) * static_cast<unsigned long long>(page_size);
}

static unsigned long long availableDiskBytes(const std::string& path)
{
  // Walk up to find the first existing ancestor directory.
  std::string p = path;
  struct stat st;
  while (!p.empty() && stat(p.c_str(), &st) != 0)
  {
    size_t slash = p.rfind('/');
    if (slash == std::string::npos) break;
    p = p.substr(0, slash);
  }
  if (p.empty()) p = "/";
  struct statvfs sv;
  if (statvfs(p.c_str(), &sv) != 0) return 0;
  return static_cast<unsigned long long>(sv.f_bavail) *
         static_cast<unsigned long long>(sv.f_frsize);
}

static bool pathIsWritable(const std::string& path)
{
  // Check the deepest existing directory in the path.
  std::string p = path;
  struct stat st;
  while (!p.empty() && stat(p.c_str(), &st) != 0)
  {
    size_t slash = p.rfind('/');
    if (slash == std::string::npos) break;
    p = p.substr(0, slash);
  }
  return !p.empty() && (access(p.c_str(), W_OK) == 0);
}

// Rough estimate of the number of workspace spheres for a given resolution.
// Based on a sphere of radius 2.5 m discretised at voxel size = resolution.
static long estimateSphereCount(double resolution)
{
  if (resolution <= 0.0) return 0;
  const double radius = 2.5;
  const double volume = (4.0 / 3.0) * 3.14159265 * radius * radius * radius;
  return static_cast<long>(volume / (resolution * resolution * resolution));
}

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

  // -------------------------------------------------------------------------
  // Pre-flight checks - run once at startup so problems surface immediately
  // rather than after hours of computation.
  // -------------------------------------------------------------------------
  bool preflight_ok = true;

  // 1. Output path writability
  if (!pathIsWritable(path_))
  {
    ROS_FATAL("Output path is not writable (and cannot be created): %s", path_.c_str());
    preflight_ok = false;
  }
  else
  {
    ROS_INFO("[preflight] Output path OK: %s", path_.c_str());
  }

  // 2. Disk space — estimate output file size conservatively at 500 MB for
  //    fine resolutions; warn below 5 GB to leave plenty of headroom.
  {
    unsigned long long disk_bytes = availableDiskBytes(path_);
    double disk_gb = disk_bytes / 1e9;
    const double warn_gb = 5.0;
    if (disk_bytes == 0)
    {
      ROS_WARN("[preflight] Could not determine available disk space at %s", path_.c_str());
    }
    else if (disk_gb < warn_gb)
    {
      ROS_WARN("[preflight] Low disk space: %.1f GB available at output path (recommend >= %.0f GB)",
               disk_gb, warn_gb);
    }
    else
    {
      ROS_INFO("[preflight] Disk space OK: %.1f GB available", disk_gb);
    }
  }

  // 3. RAM — estimate peak memory required.
  //    With incremental batch saving, sphere_maps holds only one batch at a
  //    time (~5% of spheres).  The dominant fixed costs are:
  //      - init_ws_ in ReachAbility:   est_spheres * 50 poses * 56 B  ≈ A GB
  //      - 20 per-thread robot models: ~200 MB each                   ≈ 4 GB
  //      - One batch of sphere_maps:   batch * 50 * 200 B             ≈ B MB
  //    Warn if available RAM is below estimated total + 4 GB headroom.
  {
    long est_spheres = estimateSphereCount(resolution_);
    const long poses_per_sphere = 50;
    const double bytes_per_init_ws_pose = 56.0;   // geometry_msgs::Pose
    const double bytes_per_map_entry    = 200.0;  // multimap node + 2 vectors
    const double robot_models_gb        = 4.0;

    double init_ws_gb  = (est_spheres * poses_per_sphere * bytes_per_init_ws_pose) / 1e9;
    double batch_gb    = (est_spheres / 20.0 * poses_per_sphere * bytes_per_map_entry) / 1e9;
    double est_peak_gb = init_ws_gb + batch_gb + robot_models_gb;

    unsigned long long ram_bytes = availableRamBytes();
    double ram_gb = ram_bytes / 1e9;
    const double headroom_gb = 4.0;

    ROS_INFO("[preflight] Resolution %.3f -> ~%ld spheres, estimated peak RAM: init_ws=%.1f GB, "
             "batch=%.1f GB, models=%.0f GB -> total ~%.1f GB",
             resolution_, est_spheres, init_ws_gb, batch_gb, robot_models_gb, est_peak_gb);

    if (ram_bytes == 0)
    {
      ROS_WARN("[preflight] Could not determine available RAM");
    }
    else if (ram_gb < est_peak_gb + headroom_gb)
    {
      ROS_WARN("[preflight] Available RAM (%.1f GB) may be insufficient for estimated peak "
               "(%.1f GB + %.0f GB headroom). Consider reducing resolution or num_ik_threads.",
               ram_gb, est_peak_gb, headroom_gb);
    }
    else
    {
      ROS_INFO("[preflight] RAM OK: %.1f GB available, %.1f GB estimated peak", ram_gb, est_peak_gb);
    }
  }

  if (!preflight_ok)
  {
    ROS_FATAL("Pre-flight checks failed. Aborting.");
    return 1;
  }

  reuleaux::mapGeneration mg(nh_, group_name_, ee_frame_, path_, filename_, pkg_name_,
                             resolution_, radius_, check_collision_);
  mg.generate();

  ros::spin();
  return 0;
}

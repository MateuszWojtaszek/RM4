#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from rclpy.time import Time, Duration
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy

from std_msgs.msg import Int32MultiArray
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Twist, Quaternion, Point
from sensor_msgs.msg import LaserScan

from collections import deque
import math
import random
from functools import partial

# --- Stałe Konfiguracyjne ---
NUM_ROBOTS = 16
BASE_START_X = -8.0
BASE_Y = 8.0
BASE_SPACING = 1.0

PICKUP_START_X = -8.0
PICKUP_START_Y = -8.0
PICKUP_SPACING_Y = 1.0
NUM_PICKUP_LOCATIONS = 8

DROPOFF_START_X = 8.0
DROPOFF_START_Y = -8.0
DROPOFF_SPACING_Y = 1.0
NUM_DROPOFF_LOCATIONS = 8

POSITION_TOLERANCE = 0.2
ANGLE_TOLERANCE = 0.15
LINEAR_SPEED = 0.45
ANGULAR_SPEED = 0.5
ANGULAR_P_GAIN = 1.0
WAIT_TIME_SECONDS = 2.0
VACATE_DISTANCE_THRESHOLD = 0.8

TARGET_BASE_ORIENTATION = math.pi
BASE_ORIENTATION_TOLERANCE = 0.05

# Zaktualizowane stałe dla nowego unikania kolizji
COLLISION_RESOLUTION_TIMEOUT_SECONDS = 4.0
COLLISION_MANEUVER_TURN_ANGLE = math.pi / 2
COLLISION_MANEUVER_DRIVE_DIST = 0.7
MANEUVER_TURN_SPEED_FACTOR = 0.4
MANEUVER_DRIVE_SPEED_FACTOR = 0.5
CAUTION_SPEED_FACTOR = 0.6 

# Stałe dla skanera laserowego
SCAN_CRITICAL_DISTANCE_IMMEDIATE = 0.45
SCAN_FORWARD_ARC_RAD_IMMEDIATE = math.pi / 6
AVOIDANCE_RECT_LENGTH = 1.0
AVOIDANCE_RECT_WIDTH = 0.5

ACTIVE_MANEUVER_LOCK_TIMEOUT_SECONDS = 25.0

# Stany robota
STATE_IDLE_IN_BASE = "IDLE_IN_BASE"
STATE_LEAVING_B1_FOR_TASK = "LEAVING_B1_FOR_TASK"
STATE_MOVING_TO_PICKUP = "MOVING_TO_PICKUP"
STATE_AT_PICKUP = "AT_PICKUP"
STATE_MOVING_TO_DROPOFF = "MOVING_TO_DROPOFF"
STATE_AT_DROPOFF = "AT_DROPOFF"
STATE_RETURNING_TO_BASE = "RETURNING_TO_BASE"
STATE_MOVING_IN_BASE_QUEUE = "MOVING_IN_BASE_QUEUE"
STATE_ORIENTING_IN_BASE = "ORIENTING_IN_BASE"
STATE_COLLISION_AVOIDANCE = "COLLISION_AVOIDANCE"


class RobotState:
    """! Przechowuje kompletny stan pojedynczego robota."""
    def __init__(self, robot_id: str):
        self.robot_id = robot_id
        self.current_pose = Point(x=0.0, y=0.0, z=0.0)
        self.target_pose_coords = None
        self.target_orientation_z = None
        self.state = STATE_IDLE_IN_BASE
        self.assigned_task = None
        self.path_to_follow = []
        self.current_path_segment_index = 0
        self.base_station_id = None
        self.wait_timer_start_time = None
        self.is_in_collision_avoidance = False
        self.collision_partner = None
        self.is_performing_maneuver = False
        self.maneuver_step = 0
        self.maneuver_target_yaw = None
        self.maneuver_segment_start_pose = None
        self.maneuver_target_distance = 0.0
        self.maneuver_turn_direction_coeff = -1
        self.pre_collision_state = None
        self.pre_collision_target_pose_coords = None
        self.pre_collision_path_to_follow = []
        self.pre_collision_current_path_segment_index = 0
        self.pre_collision_target_orientation_z = None
        self.is_completing_initial_return_orientation = False
        self.latest_scan: LaserScan | None = None

    def __repr__(self) -> str:
        return (f"Robot(id={self.robot_id}, state={self.state}, base_station={self.base_station_id}, "
                f"task={self.assigned_task}, target_pos={self.target_pose_coords}, target_orient={self.target_orientation_z}, "
                f"is_CIO={self.is_completing_initial_return_orientation}, "
                f"maneuver_step={self.maneuver_step if self.is_performing_maneuver else 'N/A'}, "
                f"is_CA={self.is_in_collision_avoidance}, "
                f"pose=({self.current_pose.x:.2f}, {self.current_pose.y:.2f}, {self.current_pose.z:.2f}))")

class RobotSupervisorNode(Node):
    """! Główny węzeł zarządzający flotą robotów."""
    def __init__(self):
        super().__init__('robot_supervisor_node')
        self.robots: dict[str, RobotState] = {}
        self.base_stations: list[dict] = []
        self.pickup_locations: list[dict] = []
        self.dropoff_locations: list[dict] = []
        self.task_queue: deque = deque()
        self.queue_shift_triggered_this_cycle: bool = False
        self.is_queue_shifting_active: bool = False
        self.last_queue_shift_completion_time = None
        self.queue_shift_cooldown_duration: Duration = Duration(seconds=1.0)
        self.active_base_maneuver_robot_id: str | None = None
        self.base_maneuver_start_time: Time | None = None

        self._initialize_locations()
        self._initialize_robots()
        self._initialize_ros_comms()
        timer_period = 0.01
        self.control_loop_timer = self.create_timer(timer_period, self.control_loop_callback)
        self.get_logger().info("Robot Supervisor Node gotowy.")

    def _initialize_ros_comms(self):
        self.task_subscriber = self.create_subscription(
            Int32MultiArray, '/add_task', self.new_task_callback, 10)
        self.robot_odom_subscribers = {}
        self.robot_cmd_vel_publishers = {}
        qos_sensor_data = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST, depth=1)
        for i in range(NUM_ROBOTS):
            robot_id = f"tb0_{i}"
            self.robot_odom_subscribers[robot_id] = self.create_subscription(
                Odometry, f"/{robot_id}/odom", partial(self.odometry_callback, robot_id=robot_id), 10)
            self.robot_cmd_vel_publishers[robot_id] = self.create_publisher(
                Twist, f"/{robot_id}/cmd_vel", 10)
            self.robot_odom_subscribers[f"{robot_id}_scan"] = self.create_subscription(
                LaserScan, f"/{robot_id}/scan", partial(self.scan_callback, robot_id=robot_id), qos_sensor_data)

    def _initialize_locations(self):
        for i in range(NUM_ROBOTS):
            self.base_stations.append({"id": i + 1, "coordinates": (BASE_START_X + i * BASE_SPACING, BASE_Y), "occupant_robot_id": f"tb0_{i}" })
        for i in range(NUM_PICKUP_LOCATIONS):
            self.pickup_locations.append({"id": i, "coords": (PICKUP_START_X, PICKUP_START_Y + i * PICKUP_SPACING_Y)})
        for i in range(NUM_DROPOFF_LOCATIONS):
            self.dropoff_locations.append({"id": i, "coords": (DROPOFF_START_X, DROPOFF_START_Y + i * DROPOFF_SPACING_Y)})

    def _initialize_robots(self):
        for i in range(NUM_ROBOTS):
            robot_id = f"tb0_{i}"
            self.robots[robot_id] = RobotState(robot_id)
            self.robots[robot_id].base_station_id = i + 1
            initial_x, initial_y = self.base_stations[i]["coordinates"]
            self.robots[robot_id].current_pose = Point(x=initial_x, y=initial_y, z=TARGET_BASE_ORIENTATION)
            self.robots[robot_id].target_pose_coords = self.base_stations[i]["coordinates"]

    def euler_from_quaternion(self, q: Quaternion) -> float:
        return math.atan2(2*(q.w*q.z + q.x*q.y), 1 - 2*(q.y*q.y + q.z*q.z))

    def odometry_callback(self, msg: Odometry, robot_id: str):
        if robot_id in self.robots:
            pose = msg.pose.pose
            self.robots[robot_id].current_pose.x = pose.position.x
            self.robots[robot_id].current_pose.y = pose.position.y
            self.robots[robot_id].current_pose.z = self.euler_from_quaternion(pose.orientation)

    def scan_callback(self, msg: LaserScan, robot_id: str):
        if robot_id in self.robots: self.robots[robot_id].latest_scan = msg

    def new_task_callback(self, msg: Int32MultiArray):
        if len(msg.data) == 2:
            p_idx, d_idx = msg.data[0], msg.data[1]
            if p_idx == 0: p_idx = random.randint(1, NUM_PICKUP_LOCATIONS)
            if d_idx == 0: d_idx = random.randint(1, NUM_DROPOFF_LOCATIONS)
            ap_idx, ad_idx = p_idx - 1, d_idx - 1
            if 0 <= ap_idx < NUM_PICKUP_LOCATIONS and 0 <= ad_idx < NUM_DROPOFF_LOCATIONS:
                self.task_queue.append((ap_idx, ad_idx))

    def check_queue_shift_status(self):
        if not self.is_queue_shifting_active: return
        if not any(r.state == STATE_MOVING_IN_BASE_QUEUE and r.robot_id != self.active_base_maneuver_robot_id for r in self.robots.values()):
            self.is_queue_shifting_active = False; self.last_queue_shift_completion_time = self.get_clock().now()

    def _check_maneuver_lock_timeout(self):
        if self.active_base_maneuver_robot_id and self.base_maneuver_start_time:
            elapsed_time = (self.get_clock().now() - self.base_maneuver_start_time).nanoseconds / 1e9
            if elapsed_time > ACTIVE_MANEUVER_LOCK_TIMEOUT_SECONDS:
                stuck_robot_id = self.active_base_maneuver_robot_id
                self.get_logger().error(f"TIMEOUT BLOKADY MANEWRU! Robot {stuck_robot_id} blokował przez {elapsed_time:.1f}s.")
                if stuck_robot_id in self.robots:
                    stuck_robot = self.robots[stuck_robot_id]
                    self.stop_robot(stuck_robot_id)
                    self._send_robot_to_base_on_error(stuck_robot, log_prefix=f"TimeoutBlokadyManewru-{stuck_robot_id}: ")
                    if stuck_robot.state not in [STATE_IDLE_IN_BASE, STATE_COLLISION_AVOIDANCE, STATE_MOVING_IN_BASE_QUEUE]:
                         self._set_robot_idle(stuck_robot, clear_task_if_present=True, log_message_suffix="(po timeout blokady)")
                else:
                    self.get_logger().error(f"Nie można znaleźć robota {stuck_robot_id} po timeout blokady.")
                self.active_base_maneuver_robot_id = None
                self.base_maneuver_start_time = None
                self.get_logger().info("Blokada manewru w bazie zwolniona po timeout.")

    def control_loop_callback(self):
        self.queue_shift_triggered_this_cycle = False
        self._check_maneuver_lock_timeout()
        self.check_queue_shift_status()
        self.assign_tasks()
        self.update_robots_state_and_move()

    def assign_tasks(self):
        if not self.task_queue or self.is_queue_shifting_active or self.active_base_maneuver_robot_id: return
        if self.last_queue_shift_completion_time and (self.get_clock().now() - self.last_queue_shift_completion_time) < self.queue_shift_cooldown_duration: return
        robot_at_b1_id = self.base_stations[0].get("occupant_robot_id")
        if robot_at_b1_id and self.robots[robot_at_b1_id].state == STATE_IDLE_IN_BASE:
            robot, task = self.robots[robot_at_b1_id], self.task_queue.popleft()
            robot.assigned_task = task; robot.state = STATE_LEAVING_B1_FOR_TASK
            robot.target_pose_coords = self.pickup_locations[task[0]]["coords"]
            self.plan_manhattan_path(robot.robot_id, robot.target_pose_coords)

    def _handle_state_leaving_b1(self, robot: RobotState, current_time: Time):
        arrived = self.follow_path(robot)
        b1 = self.base_stations[0]
        if b1["occupant_robot_id"] == robot.robot_id and self.calculate_distance(robot.current_pose, b1["coordinates"]) > VACATE_DISTANCE_THRESHOLD:
            b1["occupant_robot_id"] = None; robot.base_station_id = None; robot.state = STATE_MOVING_TO_PICKUP
            if not self.queue_shift_triggered_this_cycle and not self.active_base_maneuver_robot_id: self.base_queue_shift(); self.queue_shift_triggered_this_cycle = True
        elif b1["occupant_robot_id"] is None and robot.state == STATE_LEAVING_B1_FOR_TASK and not self.queue_shift_triggered_this_cycle and not self.active_base_maneuver_robot_id:
            robot.state = STATE_MOVING_TO_PICKUP; self.base_queue_shift(); self.queue_shift_triggered_this_cycle = True
        if arrived and robot.state == STATE_LEAVING_B1_FOR_TASK:
            robot.state = STATE_AT_PICKUP; robot.wait_timer_start_time = current_time
            if b1["occupant_robot_id"] == robot.robot_id:
                b1["occupant_robot_id"] = None; robot.base_station_id = None
                if not self.queue_shift_triggered_this_cycle and not self.active_base_maneuver_robot_id: self.base_queue_shift(); self.queue_shift_triggered_this_cycle = True
    
    def _handle_state_path_following(self, robot: RobotState, current_time: Time, speed_factor: float = 1.0):
        if self.follow_path(robot, speed_factor):
            self.stop_robot(robot.robot_id)
            if robot.state == STATE_MOVING_TO_PICKUP:
                robot.state = STATE_AT_PICKUP; robot.wait_timer_start_time = current_time
            elif robot.state == STATE_MOVING_TO_DROPOFF:
                robot.state = STATE_AT_DROPOFF; robot.wait_timer_start_time = current_time
            elif robot.state == STATE_RETURNING_TO_BASE:
                if self.active_base_maneuver_robot_id is None or self.active_base_maneuver_robot_id == robot.robot_id:
                    self.get_logger().info(f"Robot {robot.robot_id} dotarł na ost. stację ({robot.base_station_id}). Rozpoczyna sekwencję self-shift. BLOKADA START.")
                    self.active_base_maneuver_robot_id = robot.robot_id
                    self.base_maneuver_start_time = self.get_clock().now()
                    robot.state = STATE_ORIENTING_IN_BASE
                    robot.target_orientation_z = TARGET_BASE_ORIENTATION
                    robot.is_completing_initial_return_orientation = True
                else:
                    self.get_logger().info(f"Robot {robot.robot_id} dotarł na ost. stację, ale {self.active_base_maneuver_robot_id} manewruje. Czeka.")
            elif robot.state == STATE_MOVING_IN_BASE_QUEUE:
                self._finalize_robot_move_in_base_queue(robot)

    def _finalize_robot_move_in_base_queue(self, robot: RobotState):
        station = next((s for s in self.base_stations if s["id"] == robot.base_station_id), None)
        if station and station["occupant_robot_id"] == robot.robot_id:
            robot.state = STATE_ORIENTING_IN_BASE; robot.target_orientation_z = TARGET_BASE_ORIENTATION
            robot.is_completing_initial_return_orientation = False
            if self.active_base_maneuver_robot_id == robot.robot_id:
                 self.get_logger().info(f"Robot {robot.robot_id} (self-shift) dotarł, orientuje. Blokada aktywna.")
        else:
            self._set_robot_idle(robot, clear_task=True, log_message_suffix=f"(błąd finalizacji ruchu do stacji {robot.base_station_id})")
            if self.active_base_maneuver_robot_id == robot.robot_id:
                self.active_base_maneuver_robot_id = None; self.base_maneuver_start_time = None
        robot.path_to_follow = []; robot.current_path_segment_index = 0

    def _handle_state_at_pickup(self, robot: RobotState, current_time: Time):
        if robot.wait_timer_start_time and (current_time - robot.wait_timer_start_time) > Duration(seconds=WAIT_TIME_SECONDS):
            robot.state = STATE_MOVING_TO_DROPOFF; _, task_dropoff_idx = robot.assigned_task
            robot.target_pose_coords = self.dropoff_locations[task_dropoff_idx]["coords"]
            self.plan_manhattan_path(robot.robot_id, robot.target_pose_coords); robot.wait_timer_start_time = None

    def _handle_state_at_dropoff(self, robot: RobotState, current_time: Time):
        if robot.wait_timer_start_time and (current_time - robot.wait_timer_start_time) > Duration(seconds=WAIT_TIME_SECONDS):
            last_station = self.base_stations[NUM_ROBOTS - 1]
            can_proceed = True
            if self.active_base_maneuver_robot_id and self.active_base_maneuver_robot_id != robot.robot_id:
                maneuvering_robot = self.robots[self.active_base_maneuver_robot_id]
                if maneuvering_robot.base_station_id == last_station["id"] or \
                   (maneuvering_robot.target_pose_coords and self.calculate_distance(maneuvering_robot.target_pose_coords, last_station["coordinates"]) < POSITION_TOLERANCE):
                    robot.wait_timer_start_time = current_time; can_proceed = False
            if can_proceed:
                robot.state = STATE_RETURNING_TO_BASE; last_station["occupant_robot_id"] = robot.robot_id
                robot.base_station_id = last_station["id"]; robot.target_pose_coords = last_station["coordinates"]
                self.plan_manhattan_path(robot.robot_id, robot.target_pose_coords); robot.wait_timer_start_time = None

    def _handle_state_orienting_in_base(self, robot: RobotState):
        if robot.target_orientation_z is None:
            log_suffix = "(błąd orientacji - brak celu)"
            self._set_robot_idle(robot, clear_task=True, reset_orientation_flag=True, log_message_suffix=log_suffix)
            if self.active_base_maneuver_robot_id == robot.robot_id:
                self.active_base_maneuver_robot_id = None; self.base_maneuver_start_time = None
            return

        angle_diff = self.normalize_angle(robot.target_orientation_z - robot.current_pose.z)
        if abs(angle_diff) > BASE_ORIENTATION_TOLERANCE:
            twist = Twist(); twist.angular.z = (ANGULAR_SPEED * 0.3) * math.copysign(1, angle_diff)
            self.robot_cmd_vel_publishers[robot.robot_id].publish(twist)
        else:
            self.stop_robot(robot.robot_id); robot.target_orientation_z = None
            self.get_logger().info(f"Robot {robot.robot_id} zakończył orientację na stacji {robot.base_station_id}.")
            if robot.is_completing_initial_return_orientation:
                self._handle_initial_base_orientation_completion_and_self_shift(robot)
            else:
                is_maneuvering_robot = (self.active_base_maneuver_robot_id == robot.robot_id)
                self._set_robot_idle(robot, clear_task_if_present=True, log_message_suffix="(finalna orientacja)")
                if is_maneuvering_robot:
                    self.get_logger().info(f"Robot {robot.robot_id} zakończył self-shift i parkowanie. BLOKADA STOP.")
                    self.active_base_maneuver_robot_id = None
                    self.base_maneuver_start_time = None

    def _set_robot_idle(self, robot: RobotState, clear_task: bool = False, reset_orientation_flag: bool = False, clear_task_if_present: bool = False, log_message_suffix: str = ""):
        self.stop_robot(robot.robot_id); robot.state = STATE_IDLE_IN_BASE
        if clear_task and robot.assigned_task: robot.assigned_task = None
        elif clear_task_if_present and robot.assigned_task: robot.assigned_task = None
        if reset_orientation_flag: robot.is_completing_initial_return_orientation = False

    def _handle_initial_base_orientation_completion_and_self_shift(self, robot: RobotState):
        robot.is_completing_initial_return_orientation = False
        current_station_idx = robot.base_station_id - 1
        if self.active_base_maneuver_robot_id != robot.robot_id:
            self.get_logger().error(f"KRYTYCZNY BŁĄD LOGIKI: Robot {robot.robot_id} w _handle_initial_base_orientation_completion_and_self_shift, "
                                   f"ale active_base_maneuver_robot_id to {self.active_base_maneuver_robot_id}!")
            if self.active_base_maneuver_robot_id is None:
                self.active_base_maneuver_robot_id = robot.robot_id
                self.base_maneuver_start_time = self.get_clock().now()
            else:
                self._set_robot_idle(robot, clear_task_if_present=True, log_message_suffix="(błąd przejęcia blokady w self-shift)")
                return

        target_final_spot_idx = -1
        for i in range(NUM_ROBOTS):
            if self.base_stations[i]["occupant_robot_id"] is None:
                target_final_spot_idx = i; break
            elif self.base_stations[i]["occupant_robot_id"] == robot.robot_id and i == current_station_idx:
                 if target_final_spot_idx == -1: target_final_spot_idx = i; break
        if target_final_spot_idx == -1: target_final_spot_idx = current_station_idx
            
        if target_final_spot_idx != current_station_idx:
            self.get_logger().info(f"Robot {robot.robot_id} (manewr aktywny) rozpoczyna self-shift ze stacji {current_station_idx+1} na {target_final_spot_idx+1}.")
            if self.base_stations[current_station_idx]["occupant_robot_id"] == robot.robot_id:
                 self.base_stations[current_station_idx]["occupant_robot_id"] = None
            new_station = self.base_stations[target_final_spot_idx]
            new_station["occupant_robot_id"] = robot.robot_id
            robot.base_station_id = new_station["id"]; robot.target_pose_coords = new_station["coordinates"]
            robot.state = STATE_MOVING_IN_BASE_QUEUE
            self.plan_manhattan_path(robot.robot_id, robot.target_pose_coords)
        else:
            self._set_robot_idle(robot, clear_task=True, log_message_suffix=f"(już na finalnym miejscu {robot.base_station_id})")
            if self.active_base_maneuver_robot_id == robot.robot_id:
                self.get_logger().info(f"Robot {robot.robot_id} nie wymagał self-shiftu. BLOKADA STOP.")
                self.active_base_maneuver_robot_id = None
                self.base_maneuver_start_time = None

    def _is_immediate_path_blocked_by_scan(self, robot: RobotState) -> bool:
        if not robot.latest_scan or not robot.latest_scan.ranges: return False
        scan = robot.latest_scan
        
        for i, range_val in enumerate(scan.ranges):
            angle = scan.angle_min + i * scan.angle_increment
            if -SCAN_FORWARD_ARC_RAD_IMMEDIATE / 2.0 <= angle <= SCAN_FORWARD_ARC_RAD_IMMEDIATE / 2.0:
                if isinstance(range_val, float) and math.isfinite(range_val) and 0 < range_val < SCAN_CRITICAL_DISTANCE_IMMEDIATE:
                    return True
        return False

    def _is_obstacle_in_path_rectangle_by_scan(self, robot: RobotState) -> bool:
        if not robot.latest_scan or not robot.latest_scan.ranges:
            return False
        scan = robot.latest_scan

        for i, range_val in enumerate(scan.ranges):
            if not (isinstance(range_val, float) and math.isfinite(range_val) and range_val > 0):
                continue

            angle = scan.angle_min + i * scan.angle_increment
            point_x = range_val * math.cos(angle)
            point_y = range_val * math.sin(angle)

            if 0 < point_x < AVOIDANCE_RECT_LENGTH and abs(point_y) < AVOIDANCE_RECT_WIDTH / 2:
                return True
        return False
    
    # <<< NOWOŚĆ: Cała nowa metoda >>>
    def _find_robot_in_critical_path(self, robot: RobotState) -> RobotState | None:
        """!
        Sprawdza, czy w krytycznej ścieżce przed robotem znajduje się inny robot z floty.

        Na podstawie danych ze skanera identyfikuje punkty w strefie krytycznej,
        a następnie sprawdza, czy ich pozycja odpowiada pozycji któregokolwiek
        innego robota.

        @param robot Robot, dla którego sprawdzana jest ścieżka.
        @return Obiekt RobotState blokującego robota lub None, jeśli przeszkodą
                nie jest robot lub ścieżka jest wolna.
        """
        if not robot.latest_scan or not robot.latest_scan.ranges:
            return None

        scan = robot.latest_scan
        
        # Przeszukaj punkty w krytycznym stożku
        for i, range_val in enumerate(scan.ranges):
            angle = scan.angle_min + i * scan.angle_increment
            if not (-SCAN_FORWARD_ARC_RAD_IMMEDIATE / 2.0 <= angle <= SCAN_FORWARD_ARC_RAD_IMMEDIATE / 2.0):
                continue

            if isinstance(range_val, float) and math.isfinite(range_val) and 0 < range_val < SCAN_CRITICAL_DISTANCE_IMMEDIATE:
                # Mamy punkt krytyczny. Sprawdźmy, czy to robot.
                # Oblicz współrzędne przeszkody w układzie globalnym
                robot_angle = robot.current_pose.z
                obstacle_world_x = robot.current_pose.x + range_val * math.cos(robot_angle + angle)
                obstacle_world_y = robot.current_pose.y + range_val * math.sin(robot_angle + angle)
                
                # Porównaj z pozycjami innych robotów
                for other_robot_id, other_robot in self.robots.items():
                    if robot.robot_id == other_robot_id:
                        continue
                    
                    dist = self.calculate_distance((obstacle_world_x, obstacle_world_y), other_robot.current_pose)
                    
                    # Używamy tolerancji nieco większej niż promień robota
                    if dist < 0.3: 
                        return other_robot # Znaleziono robota blokującego

        return None

    def _initiate_unilateral_avoidance(self, robot: RobotState):
        self.stop_robot(robot.robot_id)
        self.get_logger().warn(f"Robot {robot.robot_id} inicjuje manewr unikania kolizji (w prawo).")
        
        self._save_pre_collision_state(robot)
        robot.state = STATE_COLLISION_AVOIDANCE
        robot.is_in_collision_avoidance = True
        robot.is_performing_maneuver = True
        robot.maneuver_step = 0
        robot.maneuver_turn_direction_coeff = -1
        robot.collision_partner = None
        robot.wait_timer_start_time = None

    def update_robots_state_and_move(self):
        current_time = self.get_clock().now()
        for robot_id, robot in self.robots.items():
            # Logika pauzowania innych robotów
            should_this_robot_be_paused = False
            if self.active_base_maneuver_robot_id and robot.robot_id != self.active_base_maneuver_robot_id:
                if robot.state == STATE_RETURNING_TO_BASE:
                    if robot.target_pose_coords:
                        dist_sum_to_target = abs(robot.target_pose_coords[0] - robot.current_pose.x) + abs(robot.target_pose_coords[1] - robot.current_pose.y)
                        if dist_sum_to_target < 2.0: should_this_robot_be_paused = True
                    else: should_this_robot_be_paused = True
                elif robot.state in [STATE_MOVING_IN_BASE_QUEUE, STATE_ORIENTING_IN_BASE, STATE_LEAVING_B1_FOR_TASK]:
                    should_this_robot_be_paused = True
            
            if should_this_robot_be_paused:
                self.stop_robot(robot.robot_id)
                continue
            
            # Nowa logika unikania kolizji oparta na skanerze
            is_moving_linearly = robot.state in [
                STATE_LEAVING_B1_FOR_TASK, STATE_MOVING_TO_PICKUP, STATE_MOVING_TO_DROPOFF,
                STATE_RETURNING_TO_BASE, STATE_MOVING_IN_BASE_QUEUE]

            speed_reduction_factor = 1.0

            if is_moving_linearly and robot.latest_scan:
                # <<< ZMIANA: Gruntowna przebudowa logiki wykrywania krytycznej blokady >>>
                is_critically_blocked = self._is_immediate_path_blocked_by_scan(robot)
                
                if is_critically_blocked:
                    blocking_robot = self._find_robot_in_critical_path(robot)
                    
                    # WARUNEK SPECJALNY: Czekanie w kolejce do bazy
                    if robot.state == STATE_RETURNING_TO_BASE and blocking_robot and blocking_robot.state == STATE_RETURNING_TO_BASE:
                        self.get_logger().info(f"Robot {robot.robot_id} (wraca do bazy) czeka na {blocking_robot.robot_id} (też wraca do bazy).")
                        self.stop_robot(robot.robot_id)
                        continue # Przejdź do następnego robota, ten czeka

                    # Standardowy manewr unikania przeszkody
                    else:
                        blocker_info = f"robotem {blocking_robot.robot_id} w stanie {blocking_robot.state}" if blocking_robot else "niezidentyfikowaną przeszkodą"
                        self.get_logger().warn(f"Robot {robot.robot_id} ZATRZYMANY PRZEZ SKANER - blokada przez {blocker_info}. Inicjuję manewr.")
                        self.stop_robot(robot.robot_id)
                        self._initiate_unilateral_avoidance(robot)
                        continue 
                
                is_caution_needed = self._is_obstacle_in_path_rectangle_by_scan(robot)
                if is_caution_needed:
                    speed_reduction_factor = CAUTION_SPEED_FACTOR
                    self.get_logger().info(f"Robot {robot.robot_id} zwalnia - przeszkoda w strefie ostrzegawczej.")

            # Główna maszyna stanów
            if robot.state == STATE_COLLISION_AVOIDANCE:
                self.execute_collision_avoidance_step(robot)
            elif robot.state == STATE_IDLE_IN_BASE:
                self.stop_robot(robot_id)
            elif robot.state == STATE_LEAVING_B1_FOR_TASK:
                self._handle_state_leaving_b1(robot, current_time)
            elif robot.state in [STATE_MOVING_TO_PICKUP, STATE_MOVING_TO_DROPOFF, STATE_RETURNING_TO_BASE, STATE_MOVING_IN_BASE_QUEUE]:
                self._handle_state_path_following(robot, current_time, speed_reduction_factor)
            elif robot.state == STATE_AT_PICKUP:
                self._handle_state_at_pickup(robot, current_time)
            elif robot.state == STATE_AT_DROPOFF:
                self._handle_state_at_dropoff(robot, current_time)
            elif robot.state == STATE_ORIENTING_IN_BASE:
                self._handle_state_orienting_in_base(robot)

    def plan_manhattan_path(self, robot_id: str, target_coords: tuple):
        robot = self.robots[robot_id]; robot.path_to_follow = []; robot.current_path_segment_index = 0
        curr_x, curr_y = robot.current_pose.x, robot.current_pose.y
        tgt_x, tgt_y = target_coords
        if abs(tgt_x - curr_x) > POSITION_TOLERANCE: robot.path_to_follow.append((tgt_x, curr_y))
        if abs(tgt_y - curr_y) > POSITION_TOLERANCE or not robot.path_to_follow:
            if not robot.path_to_follow or robot.path_to_follow[-1] != (tgt_x, tgt_y):
                 robot.path_to_follow.append((tgt_x, tgt_y))
        if not robot.path_to_follow: robot.path_to_follow.append((tgt_x, tgt_y))

    def follow_path(self, robot: RobotState, speed_factor: float = 1.0) -> bool:
        if not robot.path_to_follow or robot.current_path_segment_index >= len(robot.path_to_follow):
            return True
        tgt_x, tgt_y = robot.path_to_follow[robot.current_path_segment_index]
        curr_x, curr_y, curr_theta = robot.current_pose.x, robot.current_pose.y, robot.current_pose.z
        if self.calculate_distance((curr_x, curr_y), (tgt_x, tgt_y)) < POSITION_TOLERANCE:
            robot.current_path_segment_index += 1
            self.stop_robot(robot.robot_id)
            return robot.current_path_segment_index >= len(robot.path_to_follow)

        twist = Twist()
        angle_to_target = math.atan2(tgt_y - curr_y, tgt_x - curr_x)
        angle_diff = self.normalize_angle(angle_to_target - curr_theta)

        current_linear_speed = LINEAR_SPEED * speed_factor
        current_angular_speed = ANGULAR_SPEED * speed_factor

        if abs(angle_diff) > math.pi * 0.66:
            twist.angular.z = current_angular_speed * math.copysign(1, angle_diff)
        else:
            twist.angular.z = max(-current_angular_speed, min(current_angular_speed, ANGULAR_P_GAIN * angle_diff))
            if abs(twist.angular.z) < 0.05: twist.angular.z = 0.0
            
            if abs(twist.angular.z) > current_angular_speed * 0.6:
                twist.linear.x = current_linear_speed * 0.5
            elif abs(twist.angular.z) > current_angular_speed * 0.25:
                twist.linear.x = current_linear_speed * 0.75
            else:
                twist.linear.x = current_linear_speed
                
        self.robot_cmd_vel_publishers[robot.robot_id].publish(twist)
        return False

    def stop_robot(self, robot_id: str):
        if robot_id in self.robot_cmd_vel_publishers: self.robot_cmd_vel_publishers[robot_id].publish(Twist())

    def normalize_angle(self, angle: float) -> float:
        while angle > math.pi: angle -= 2.0 * math.pi
        while angle < -math.pi: angle += 2.0 * math.pi
        return angle

    def base_queue_shift(self):
        if self.active_base_maneuver_robot_id: return
        any_moved = False
        for i in range(NUM_ROBOTS - 1):
            if self.base_stations[i]["occupant_robot_id"] is None and self.base_stations[i+1]["occupant_robot_id"]:
                robot_id = self.base_stations[i+1]["occupant_robot_id"]
                robot = self.robots[robot_id]
                if robot.state == STATE_IDLE_IN_BASE:
                    self.base_stations[i]["occupant_robot_id"] = robot_id
                    self.base_stations[i+1]["occupant_robot_id"] = None
                    robot.state = STATE_MOVING_IN_BASE_QUEUE
                    robot.target_pose_coords = self.base_stations[i]["coordinates"]
                    robot.base_station_id = self.base_stations[i]["id"]
                    self.plan_manhattan_path(robot_id, robot.target_pose_coords)
                    any_moved = True
        if any_moved and not self.is_queue_shifting_active:
            self.is_queue_shifting_active = True; self.last_queue_shift_completion_time = None

    def find_end_of_queue_base_station(self) -> tuple[tuple[float,float] | None, int | None]:
        for i in range(NUM_ROBOTS - 1, -1, -1):
            if self.base_stations[i]["occupant_robot_id"] is None:
                return self.base_stations[i]["coordinates"], self.base_stations[i]["id"]
        return None, None

    def calculate_distance(self, p1, p2) -> float:
        x1,y1 = (p1.x,p1.y) if isinstance(p1,Point) else p1
        x2,y2 = (p2.x,p2.y) if isinstance(p2,Point) else p2
        return math.sqrt((x1-x2)**2 + (y1-y2)**2)

    def _save_pre_collision_state(self, r: RobotState): r.__dict__.update({f"pre_collision_{k}": getattr(r, k) for k in ["state","target_pose_coords","path_to_follow","current_path_segment_index","target_orientation_z"]})
    def _clear_pre_collision_state(self, r: RobotState): [r.__setattr__(f"pre_collision_{k}", [] if k == "path_to_follow" else None) for k in ["state","target_pose_coords","path_to_follow","current_path_segment_index","target_orientation_z"]]
    def _restore_pre_collision_state(self, r: RobotState, prefix=""):
        if r.pre_collision_state and r.pre_collision_target_pose_coords:
            r.state = r.pre_collision_state; r.target_pose_coords = r.pre_collision_target_pose_coords
            r.path_to_follow = list(r.pre_collision_path_to_follow); r.current_path_segment_index = r.pre_collision_current_path_segment_index
            r.target_orientation_z = r.pre_collision_target_orientation_z
            if not r.path_to_follow and r.target_pose_coords and r.state not in [STATE_ORIENTING_IN_BASE, STATE_IDLE_IN_BASE, STATE_AT_PICKUP, STATE_AT_DROPOFF]:
                self.plan_manhattan_path(r.robot_id, r.target_pose_coords)
        else: self._send_robot_to_base_on_error(r, prefix)
        self._clear_pre_collision_state(r)

    def _send_robot_to_base_on_error(self, robot: RobotState, log_prefix: str = ""):
        coords, station_id = self.find_end_of_queue_base_station()
        if station_id is not None and coords is not None:
            chosen_station_obj = next((s for s in self.base_stations if s["id"] == station_id), None)
            if chosen_station_obj:
                self.get_logger().info(f"{log_prefix}Robot {robot.robot_id} awaryjnie kieruje się do wolnej stacji {station_id}.")
                chosen_station_obj["occupant_robot_id"] = robot.robot_id
                robot.base_station_id = chosen_station_obj["id"]
                robot.target_pose_coords = chosen_station_obj["coordinates"]
                robot.state = STATE_MOVING_IN_BASE_QUEUE
                robot.is_completing_initial_return_orientation = False
                self.plan_manhattan_path(robot.robot_id, robot.target_pose_coords)
            else:
                self._set_robot_idle(robot, clear_task_if_present=True, log_message_suffix=f"({log_prefix}błąd odnajdywania stacji {station_id})")
        else:
            self._set_robot_idle(robot, clear_task_if_present=True, log_message_suffix=f"({log_prefix}brak wolnych stacji po błędzie)")

    def _execute_maneuver_step(self, r: RobotState) -> bool:
        step = r.maneuver_step; t=Twist()
        if step==0: r.maneuver_target_yaw = self.normalize_angle(r.current_pose.z + COLLISION_MANEUVER_TURN_ANGLE*r.maneuver_turn_direction_coeff); r.maneuver_step=1; self.stop_robot(r.robot_id)
        elif step==1:
            diff = self.normalize_angle(r.maneuver_target_yaw - r.current_pose.z)
            if abs(diff) > BASE_ORIENTATION_TOLERANCE: t.angular.z = (ANGULAR_SPEED*MANEUVER_TURN_SPEED_FACTOR)*math.copysign(1,diff); self.robot_cmd_vel_publishers[r.robot_id].publish(t)
            else: r.maneuver_step=2; self.stop_robot(r.robot_id)
        elif step==2: r.maneuver_segment_start_pose = Point(x=r.current_pose.x,y=r.current_pose.y,z=r.current_pose.z); r.maneuver_target_distance=COLLISION_MANEUVER_DRIVE_DIST; r.maneuver_step=3; self.stop_robot(r.robot_id)
        elif step==3:
            if self.calculate_distance(r.current_pose,r.maneuver_segment_start_pose) < r.maneuver_target_distance:
                t.linear.x=LINEAR_SPEED*MANEUVER_DRIVE_SPEED_FACTOR
                diff_drv = self.normalize_angle(r.maneuver_segment_start_pose.z - r.current_pose.z)
                if abs(diff_drv) > ANGLE_TOLERANCE*0.75: t.angular.z=(ANGULAR_SPEED*0.15)*math.copysign(1,diff_drv)
                self.robot_cmd_vel_publishers[r.robot_id].publish(t)
            else: r.maneuver_step=4; self.stop_robot(r.robot_id)
        elif step==4:
            r.is_performing_maneuver=False; r.maneuver_step=0; r.is_in_collision_avoidance=False
            r.wait_timer_start_time=None;
            self.get_logger().info(f"Robot {r.robot_id} zakończył manewr, przywraca poprzedni stan.")
            self._restore_pre_collision_state(r);
            return True
        return False

    def execute_collision_avoidance_step(self, r: RobotState):
        if not r.is_in_collision_avoidance:
             self._restore_pre_collision_state(r)
             return
        if r.is_performing_maneuver:
            if self._execute_maneuver_step(r):
                return
        else:
             self.get_logger().warn(f"Robot {r.robot_id} w stanie CA, ale nie wykonuje manewru. Przywracanie stanu.")
             r.is_in_collision_avoidance=False
             self._restore_pre_collision_state(r)

def main(args=None):
    rclpy.init(args=args)
    node = RobotSupervisorNode()
    try: rclpy.spin(node)
    except KeyboardInterrupt: pass
    finally:
        if rclpy.ok():
            node.get_logger().info("Zatrzymywanie robotów i węzła...")
            for robot_id in list(node.robots.keys()): node.stop_robot(robot_id)
            node.destroy_node()
            rclpy.shutdown()
    node.get_logger().info("Robot Supervisor zakończył działanie.")

if __name__ == '__main__':
    main()
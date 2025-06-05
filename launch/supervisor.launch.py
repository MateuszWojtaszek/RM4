from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    return LaunchDescription([
        Node(
            package='robot_supervisor',
            executable='supervisor_node', # Nazwa skryptu po zbudowaniu
            name='robot_supervisor_node',
            output='screen'
        )
    ])
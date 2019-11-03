
import java.io.Serializable;

public class Vehicle {
        public int eventTime;
        public int vehicleID;
        public int speed;
        public int xway;
        public int lane;
        public int direction;
        public int segment;
        public int position;

        public Vehicle(){}
        public Vehicle(int eventTime, int vehicleID, int speed, int xway,int lane,int direction,int segment,int position) {
            this.eventTime = eventTime;
            this.vehicleID = vehicleID;
            this.speed = speed;
            this.xway = xway;
            this.lane = lane;
            this.direction = direction;
            this.segment = segment;
            this.position = position;
        }

        public int getTimeStamp() { return eventTime; }
        public void setTimeStamp(int eventTime) {this.eventTime=eventTime;}

        public int getID() {
            return vehicleID;
        }
        public void setID(int vehicleID) {this.vehicleID=vehicleID;}

        public int getSpeed() {
            return speed;
        }
        public void setSpeed(int speed) {this.speed=speed;}

        public int getXway(){
            return xway;
        }
        public void setXway(int xway) {this.xway=xway;}

        public int getLane(){
            return lane;
        }
        public void setLane(int lane) {this.lane=lane;}

        public int getDirection() { return direction;}
        public void setDirection(int direction) {this.direction=direction;}

        public int getSegment() {return segment;}
        public void setSegment(int segment) {this.segment=segment;}

        public int  getPosition() {return position;}
        public void setPosition(int position) {this.position=position;}
}

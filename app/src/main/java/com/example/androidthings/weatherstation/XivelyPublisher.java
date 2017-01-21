package com.example.androidthings.weatherstation;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;

import com.xively.XiException;
import com.xively.XiServiceCreatorCallback;
import com.xively.XiSession;
import com.xively.auth.XiAuthentication;
import com.xively.auth.XiAuthenticationCallback;
import com.xively.auth.XiAuthenticationFactory;
import com.xively.messaging.XiMessaging;
import com.xively.messaging.XiMessagingCreator;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by ainacio on 1/21/17.
 */


class XivelyPublisher {
    private static final String TAG = XivelyPublisher.class.getSimpleName();

    private final Context mContext;
    private final String mAccountId;
    private final String mDeviceId;
    private final String mPressureChannel;
    private final String mTemperatureChannel;
    private final String mUsername;
    private final String mPassword;

    private Handler mHandler;
    private HandlerThread mHandlerThread;

    private float mLastTemperature = Float.NaN;
    private float mLastPressure = Float.NaN;

    private static final long PUBLISH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long FIRST_PUBLISH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(15);

    private XiSession mXiSession;
    private XiMessaging mXiMessaging;

    XivelyPublisher(Context context, String accountId, String username, String password, String deviceId) throws IOException {
        mContext = context;
        mUsername = username;
        mPassword = password;
        mAccountId = accountId;
        mDeviceId = deviceId;
        mPressureChannel = "xi/blue/v1/" + mAccountId + "/d/" + mDeviceId + "/pressure";
        mTemperatureChannel = "xi/blue/v1/" + mAccountId + "/d/" + mDeviceId + "/temperature";

        mHandlerThread = new HandlerThread("xivelyPublisherThread");
        mHandlerThread.start();
        mHandler = new Handler(mHandlerThread.getLooper());


        mHandler.post(new Runnable() {
            @Override
            public void run() {

                XiAuthenticationCallback xiAuthCallback = new XiAuthenticationCallback() {
                    @Override
                    public void sessionCreated(XiSession xiSession) {
                        Log.d(TAG, "Xively Authenticated");
                        mXiSession = xiSession;

                        final XiMessagingCreator xiMessagingCreator= mXiSession.requestMessaging();

                        xiMessagingCreator.addServiceCreatorCallback(new XiServiceCreatorCallback<XiMessaging>() {
                            @Override
                            public void onServiceCreated(XiMessaging xiMessaging) {
                                xiMessagingCreator.removeAllCallbacks();
                                mXiMessaging = xiMessaging;
                                Log.d(TAG, "messaging Service Created");

                                //create a simple string message
                                byte[] byteArrayMessage = "Hello Xively World!".getBytes();
                                try {
                                    mXiMessaging.publish(mPressureChannel, byteArrayMessage, XiMessaging.XiMessagingQoS.AtLeastOnce);
                                } catch (XiException.NotConnectedException e){
                                    Log.d(TAG, "not connected exception", e);
                                }
                            }

                            @Override
                            public void onServiceCreateFailed() {
                                xiMessagingCreator.removeAllCallbacks();
                                Log.d(TAG, "failed to create messaging service");
                            }
                        });

                        xiMessagingCreator.createMessaging();
                    }

                    @Override
                    public void authenticationFailed(XiAuthenticationError xiAuthenticationError) {
                        Log.d(TAG, "Xively Authentication Failed");
                    }
                };

                XiAuthentication xiAuthentication =
                        XiAuthenticationFactory.createAuthenticationService(mContext);

                xiAuthentication.requestAuth(mUsername, mPassword, mAccountId, xiAuthCallback);
            }
        });


    }

    public void start() {
        mHandler.postDelayed(mPublishRunnable, FIRST_PUBLISH_INTERVAL_MS);
    }

    public void stop() {
        mHandler.removeCallbacks(mPublishRunnable);
    }

    public void close() {
        mHandler.removeCallbacks(mPublishRunnable);
        mHandlerThread.quitSafely();
    }

    public SensorEventListener getTemperatureListener() {
        return mTemperatureListener;
    }

    public SensorEventListener getPressureListener() {
        return mPressureListener;
    }

    private Runnable mPublishRunnable = new Runnable() {
        @Override
        public void run() {
            ConnectivityManager connectivityManager =
                    (ConnectivityManager) mContext.getSystemService(Context.CONNECTIVITY_SERVICE);
            NetworkInfo activeNetwork = connectivityManager.getActiveNetworkInfo();
            if (activeNetwork == null || !activeNetwork.isConnectedOrConnecting()) {
                Log.e(TAG, "no active network");
                return;
            }

            if (mXiMessaging == null) {
                Log.e(TAG, "Xively Connection not stablished");
                return;
            }

            try {
                JSONObject tempMessagePayload = createMessagePayload(mLastTemperature, "temperature");
                JSONObject pressureMessagePayload = createMessagePayload(mLastPressure, "pressure");

                byte[] tempMessage = getMessageBytes(tempMessagePayload);
                byte[] pressureMessage = getMessageBytes(pressureMessagePayload);

                if (!tempMessagePayload.has("data") && !pressureMessagePayload.has("data")) {
                    Log.d(TAG, "no sensor measurement to publish");
                    return;
                }

                if (tempMessagePayload.has("data")) {
                    try {
                        mXiMessaging.publish(mTemperatureChannel, tempMessage, XiMessaging.XiMessagingQoS.AtLeastOnce);
                        Log.d(TAG, "publishing message: " + tempMessagePayload);
                    }
                    catch (XiException e) {
                        Log.d(TAG, "Failed to publish Temperature: data", e);
                    }
                }
                if (pressureMessagePayload.has("data")) {
                    try {
                        mXiMessaging.publish(mPressureChannel, pressureMessage, XiMessaging.XiMessagingQoS.AtLeastOnce);
                        Log.d(TAG, "publishing message: " + pressureMessagePayload);
                    }
                    catch (XiException e) {
                        Log.d(TAG, "Failed to publish Pressure data: ", e);
                    }
                }
            } catch (JSONException e) {
                Log.e(TAG, "Error publishing message", e);
            } finally {
                mHandler.postDelayed(mPublishRunnable, PUBLISH_INTERVAL_MS);
            }
        }

        private JSONObject createMessagePayload(float data, String sensor)
                throws JSONException {
            JSONObject sensorData = new JSONObject();
            if (!Float.isNaN(data)) {
                sensorData.put(sensor, String.valueOf(data));
            }
            JSONObject messagePayload = new JSONObject();
            messagePayload.put("deviceId", mDeviceId);
            messagePayload.put("timestamp", System.currentTimeMillis());
            if (sensorData.has(sensor)) {
                messagePayload.put("data", sensorData);
            }
            return messagePayload;
        }

        private JSONObject createLogPayload(int code, String message, String severity) throws JSONException{
            JSONObject logData = new JSONObject();
            logData.put("sourceTimestamp", System.currentTimeMillis());
            logData.put("code", String.valueOf(code));
            logData.put("message", message);
            logData.put("severity", severity);

            return logData;
        }

        private byte[] getMessageBytes(JSONObject payload){
            return payload.toString().getBytes();
        }
    };

    private SensorEventListener mTemperatureListener = new SensorEventListener() {
        @Override
        public void onSensorChanged(SensorEvent event) {
            mLastTemperature = event.values[0];
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {}
    };

    private SensorEventListener mPressureListener = new SensorEventListener() {
        @Override
        public void onSensorChanged(SensorEvent event) {
            mLastPressure = event.values[0];
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {}
    };

}

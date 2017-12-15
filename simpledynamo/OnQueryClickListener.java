package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

/**
 * Created by Yang on 4/26/2016.
 */
public class OnQueryClickListener implements View.OnClickListener {
    private static final String TAG = OnQueryClickListener.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final EditText keyText;
    private String key;
    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public OnQueryClickListener(EditText _et, TextView _tv, ContentResolver _cr) {
        keyText = _et;
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        key = keyText.getText().toString();
        keyText.setText("");
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, key);
    }

    private class Task extends AsyncTask<String, Cursor, Void> {
        @Override
        protected Void doInBackground(String... params) {
            String key = params[0];
            Log.v("QueryButton", key);
            try {
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        key, null, null);
                //if (resultCursor == null) {
                //    Log.e(TAG, "Result null");
                //    throw new Exception();
                //}

                //int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                //int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                //if (keyIndex == -1 || valueIndex == -1) {
                //    Log.e(TAG, "Wrong columns");
                //    resultCursor.close();
                //    throw new Exception();
                //}

                publishProgress(resultCursor);

            } catch (Exception e) {
                Log.e(TAG, "Query fail");
                return null;
            }

            return null;
        }

        protected void onProgressUpdate(Cursor...crs) {
            mTextView.append("Query "+key+" :\n");

            Cursor resultCursor = crs[0];
            if (resultCursor==null) {
                mTextView.append("Query failed.\n");
                return;
            }
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
            resultCursor.moveToFirst();
            while (!resultCursor.isAfterLast()) {
                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                mTextView.append(returnKey+" "+returnValue+"\n");
                resultCursor.moveToNext();
            }
        }
    }
}

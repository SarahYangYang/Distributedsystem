package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

/**
 * Created by Yang on 3/26/2016.
 */
public class OnLDumpClickListener implements OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public OnLDumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, Cursor, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            String key = "@";
            try {
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        key, null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Wrong columns");
                    resultCursor.close();
                    throw new Exception();
                }

                publishProgress(resultCursor);

            } catch (Exception e) {
                Log.e(TAG, "Query @ fail");
                return null;
            }

            return null;
        }

        protected void onProgressUpdate(Cursor...crs) {
            mTextView.append("Query @:\n");

            Cursor resultCursor = crs[0];
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
            resultCursor.moveToFirst();
            while (!resultCursor.isAfterLast()) {
                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                mTextView.append(returnKey+" "+returnValue+"\n");
                resultCursor.moveToNext();
            }

            return;
        }
    }
}

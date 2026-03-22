import { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { CheckCircle, AlertCircle, Loader2, CreditCard } from 'lucide-react';
import { authFetch } from '../api/client';

/**
 * OAuth callback page for GoCardless partner signup.
 * GoCardless redirects here after the merchant completes registration.
 * This page exchanges the authorisation code for an access token via the backend.
 */
export function GoCardlessCallback() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const [status, setStatus] = useState<'processing' | 'success' | 'error'>('processing');
  const [message, setMessage] = useState('');
  const [orgName, setOrgName] = useState('');

  useEffect(() => {
    const code = searchParams.get('code');
    const state = searchParams.get('state');
    const error = searchParams.get('error');

    if (error) {
      setStatus('error');
      setMessage(searchParams.get('error_description') || `GoCardless returned an error: ${error}`);
      return;
    }

    if (!code) {
      setStatus('error');
      setMessage('No authorisation code received from GoCardless.');
      return;
    }

    // Exchange the code for an access token via our backend
    authFetch(`/api/gocardless/partner/callback?code=${encodeURIComponent(code)}&state=${encodeURIComponent(state || '')}`)
      .then(res => res.json())
      .then(data => {
        if (data.success) {
          setStatus('success');
          setMessage(data.message || 'GoCardless account connected successfully.');
          setOrgName(data.organisation_name || '');
        } else {
          setStatus('error');
          setMessage(data.error || 'Failed to complete GoCardless setup.');
        }
      })
      .catch(() => {
        setStatus('error');
        setMessage('Failed to connect to server.');
      });
  }, [searchParams]);

  return (
    <div className="max-w-md mx-auto py-20">
      <div className="text-center mb-8">
        <div className="inline-flex p-4 rounded-2xl bg-emerald-50 mb-4">
          <CreditCard className="h-10 w-10 text-emerald-500" />
        </div>
        <h1 className="text-2xl font-bold text-gray-900">GoCardless Setup</h1>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-6 text-center space-y-4">
        {status === 'processing' && (
          <>
            <Loader2 className="h-10 w-10 text-blue-500 animate-spin mx-auto" />
            <h2 className="text-lg font-semibold text-gray-800">Completing Setup...</h2>
            <p className="text-sm text-gray-500">Connecting your GoCardless account. This will only take a moment.</p>
          </>
        )}

        {status === 'success' && (
          <>
            <CheckCircle className="h-12 w-12 text-emerald-500 mx-auto" />
            <h2 className="text-lg font-semibold text-gray-800">GoCardless Connected</h2>
            {orgName && <p className="text-sm text-gray-600 font-medium">{orgName}</p>}
            <p className="text-sm text-gray-500">{message}</p>
            <div className="flex gap-3 justify-center pt-2">
              <button
                onClick={() => navigate('/cashbook/gocardless')}
                className="btn btn-primary"
              >
                Go to GoCardless Import
              </button>
              <button
                onClick={() => navigate('/cashbook/gocardless-settings')}
                className="btn btn-secondary"
              >
                Configure Settings
              </button>
            </div>
          </>
        )}

        {status === 'error' && (
          <>
            <AlertCircle className="h-12 w-12 text-red-500 mx-auto" />
            <h2 className="text-lg font-semibold text-gray-800">Setup Failed</h2>
            <p className="text-sm text-red-600">{message}</p>
            <div className="flex gap-3 justify-center pt-2">
              <button
                onClick={() => navigate('/cashbook/gocardless')}
                className="btn btn-primary"
              >
                Try Again
              </button>
              <button
                onClick={() => navigate('/cashbook/gocardless-settings')}
                className="btn btn-secondary"
              >
                Enter API Key Manually
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

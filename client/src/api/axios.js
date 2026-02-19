import axios from 'axios';
import { config } from '../config';
import { loadCache, saveCache } from '../utils/httpCache';

const api = axios.create({
    baseURL: config.API_URL
});

// Only cache a few read-heavy endpoints.
const CACHEABLE_GETS = new Set([
    '/api/board',
    '/api/ncaam/top-picks',
    '/api/ncaam/history'
]);

// Request Interceptor: Inject Token Dynamically
api.interceptors.request.use(
    (cfg) => {
        const password = localStorage.getItem('basement_password');
        if (password) {
            cfg.headers['X-BASEMENT-KEY'] = password;
        }

        // ETag revalidation for a small set of heavy GET endpoints.
        try {
            const method = (cfg.method || 'get').toLowerCase();
            const url = cfg.url;
            if (method === 'get' && url && CACHEABLE_GETS.has(url)) {
                const cached = loadCache(url, cfg.params);
                if (cached?.etag) {
                    cfg.headers['If-None-Match'] = cached.etag;
                }
            }
        } catch {
            // ignore
        }

        return cfg;
    },
    (error) => Promise.reject(error)
);

// Response Interceptor
api.interceptors.response.use(
    (response) => {
        try {
            const method = (response?.config?.method || 'get').toLowerCase();
            const url = response?.config?.url;
            if (method === 'get' && url && CACHEABLE_GETS.has(url)) {
                const etag = response?.headers?.etag;
                if (etag) {
                    saveCache(url, response?.config?.params, response.data, etag);
                }
            }
        } catch {
            // ignore
        }
        return response;
    },
    (error) => {
        // If server returns 304, Axios may treat it as error depending on config.
        // Try to serve cached payload.
        try {
            const status = error?.response?.status;
            const cfg = error?.config;
            const method = (cfg?.method || 'get').toLowerCase();
            const url = cfg?.url;
            if (status === 304 && method === 'get' && url && CACHEABLE_GETS.has(url)) {
                const cached = loadCache(url, cfg.params);
                if (cached?.payload != null) {
                    return Promise.resolve({
                        ...error.response,
                        status: 200,
                        data: cached.payload,
                        _fromCache: true,
                    });
                }
            }
        } catch {
            // ignore
        }
        return Promise.reject(error);
    }
);

export default api;

/**
 * Centralized Environment Configuration
 */

const env = import.meta.env;

export const config = {
    APP_ENV: env.VITE_PUBLIC_APP_ENV || 'local',
    export const config = {
        APP_ENV: env.VITE_PUBLIC_APP_ENV || 'local',
        API_URL: env.VITE_API_URL || '',

        // Derived properties
        isProd: (env.VITE_PUBLIC_APP_ENV || 'local') === 'prod',
        isLocal: (env.VITE_PUBLIC_APP_ENV || 'local') === 'local',
        isPreview: (env.VITE_PUBLIC_APP_ENV || 'local') === 'preview',
    };

    if(!config.isProd) {
        console.info(`[ENV] Running in ${config.APP_ENV} mode.`);
}

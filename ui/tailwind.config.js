/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/**/*.{html,js,svelte,ts}"],
  darkMode: "media",
  theme: {
    extend: {
      colors: {
        brain: {
          50: "#fef3c7",
          100: "#fde68a",
          400: "#facc15",
          500: "#eab308",
          600: "#ca8a04",
        },
      },
      animation: {
        pulse: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        beat: "beat 1s ease-out",
      },
      keyframes: {
        beat: {
          "0%": { transform: "scale(1)", opacity: "0.6" },
          "50%": { transform: "scale(1.4)", opacity: "0.2" },
          "100%": { transform: "scale(1)", opacity: "0" },
        },
      },
    },
  },
  plugins: [],
};

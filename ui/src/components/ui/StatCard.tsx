import { useEffect, useState, useRef, type ReactNode } from "react";

interface AnimatedStatProps {
  value: number;
  label: string;
  suffix?: string;
  prefix?: string;
  icon?: ReactNode;
  decimals?: number;
  duration?: number;
  color?: string;
  size?: "sm" | "md" | "lg";
}

export function AnimatedStat({
  value,
  label,
  suffix = "",
  prefix = "",
  icon,
  decimals = 0,
  duration = 1000,
  color,
  size = "md",
}: AnimatedStatProps) {
  const [displayValue, setDisplayValue] = useState(0);
  const [hasAnimated, setHasAnimated] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => {
        if (entries[0].isIntersecting && !hasAnimated) {
          setHasAnimated(true);
          animateValue();
        }
      },
      { threshold: 0.1 }
    );

    if (ref.current) {
      observer.observe(ref.current);
    }

    return () => observer.disconnect();
  }, [hasAnimated, value]);

  const animateValue = () => {
    const startTime = Date.now();
    const startValue = 0;

    const animate = () => {
      const elapsed = Date.now() - startTime;
      const progress = Math.min(elapsed / duration, 1);

      // Ease out cubic
      const easeOut = 1 - Math.pow(1 - progress, 3);
      const currentValue = startValue + (value - startValue) * easeOut;

      setDisplayValue(currentValue);

      if (progress < 1) {
        requestAnimationFrame(animate);
      } else {
        setDisplayValue(value);
      }
    };

    requestAnimationFrame(animate);
  };

  const sizeClasses = {
    sm: {
      value: "text-xl",
      label: "text-xs",
      icon: "w-4 h-4",
      padding: "p-3",
    },
    md: {
      value: "text-2xl",
      label: "text-sm",
      icon: "w-5 h-5",
      padding: "p-4",
    },
    lg: {
      value: "text-3xl",
      label: "text-base",
      icon: "w-6 h-6",
      padding: "p-5",
    },
  };

  const styles = sizeClasses[size];

  return (
    <div
      ref={ref}
      className={`glass-card rounded-xl ${styles.padding} border border-[var(--border)] hover:border-[var(--primary)]/50 transition-all duration-300 hover:transform hover:scale-[1.02]`}
    >
      {icon && (
        <div
          className={`${styles.icon} mb-2`}
          style={{ color: color || "var(--primary)" }}
        >
          {icon}
        </div>
      )}
      <p className={`${styles.value} font-bold text-[var(--text)]`}>
        {prefix}
        {displayValue.toFixed(decimals)}
        {suffix}
      </p>
      <p className={`${styles.label} text-[var(--text-muted)] mt-1`}>{label}</p>
    </div>
  );
}

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon?: ReactNode;
  trend?: {
    value: number;
    label: string;
    positive?: boolean;
  };
  color?: string;
  className?: string;
}

export function StatCard({
  title,
  value,
  subtitle,
  icon,
  trend,
  color,
  className = "",
}: StatCardProps) {
  return (
    <div
      className={`glass-card rounded-xl p-5 border border-[var(--border)] hover:border-[var(--primary)]/50 transition-all duration-300 ${className}`}
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-[var(--text-muted)]">{title}</p>
          <p className="text-2xl font-bold text-[var(--text)] mt-1">{value}</p>
          {subtitle && (
            <p className="text-xs text-[var(--text-muted)] mt-1">{subtitle}</p>
          )}
        </div>
        {icon && (
          <div
            className="p-2 rounded-lg bg-[var(--surface-hover)]"
            style={{ color: color || "var(--primary)" }}
          >
            {icon}
          </div>
        )}
      </div>
      {trend && (
        <div
          className={`flex items-center gap-1 mt-3 text-xs ${
            trend.positive ? "text-green-400" : "text-red-400"
          }`}
        >
          <span>
            {trend.positive ? "↑" : "↓"} {Math.abs(trend.value)}%
          </span>
          <span className="text-[var(--text-muted)]">{trend.label}</span>
        </div>
      )}
    </div>
  );
}

// Glassmorphism card wrapper
export function GlassCard({
  children,
  className = "",
  hover = true,
}: {
  children: ReactNode;
  className?: string;
  hover?: boolean;
}) {
  return (
    <div
      className={`
      glass-card rounded-xl border border-[var(--border)] 
      ${
        hover
          ? "hover:border-[var(--primary)]/50 transition-all duration-300 hover:transform hover:scale-[1.01]"
          : ""
      }
      ${className}
    `}
    >
      {children}
    </div>
  );
}

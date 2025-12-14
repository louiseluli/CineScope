import { useMemo } from "react";
import { Star } from "lucide-react";

interface DataPoint {
  x: number | string;
  y: number;
  label?: string;
  secondary?: number;
}

interface LineChartProps {
  data: DataPoint[];
  height?: number;
  showArea?: boolean;
  showDots?: boolean;
  showSecondary?: boolean;
  color?: string;
  secondaryColor?: string;
  xLabel?: string;
  yLabel?: string;
  title?: string;
  formatY?: (value: number) => string;
}

export function LineChart({
  data,
  height = 200,
  showArea = true,
  showDots = true,
  showSecondary = false,
  color = "var(--primary)",
  secondaryColor = "#f59e0b",
  title,
  formatY = v => v.toFixed(1),
}: LineChartProps) {
  const { points, areaPath, linePath, yMin, yMax, secondaryPath } =
    useMemo(() => {
      if (data.length === 0)
        return {
          points: [],
          areaPath: "",
          linePath: "",
          yMin: 0,
          yMax: 10,
          secondaryPath: "",
        };

      const padding = { top: 20, right: 20, bottom: 40, left: 50 };
      const chartHeight = height - padding.top - padding.bottom;

      const yValues = data
        .map(d => d.y)
        .filter(y => y !== null && y !== undefined);
      const secondaryValues = showSecondary
        ? (data
            .map(d => d.secondary)
            .filter(y => y !== null && y !== undefined) as number[])
        : [];

      const allYValues = [...yValues, ...secondaryValues];
      const yMin = Math.min(...allYValues) * 0.9;
      const yMax = Math.max(...allYValues) * 1.1;
      const yRange = yMax - yMin || 1;

      const points = data.map((d, i) => ({
        x: (i / (data.length - 1)) * 100 || 0,
        y: ((yMax - d.y) / yRange) * chartHeight + padding.top,
        secondaryY:
          d.secondary !== undefined
            ? ((yMax - d.secondary) / yRange) * chartHeight + padding.top
            : undefined,
        value: d.y,
        secondaryValue: d.secondary,
        label: d.label || String(d.x),
      }));

      const linePath = points
        .map((p, i) => `${i === 0 ? "M" : "L"} ${p.x}% ${p.y}`)
        .join(" ");

      const areaPath =
        points.length > 0
          ? `${linePath} L ${points[points.length - 1].x}% ${
              height - padding.bottom
            } L ${points[0].x}% ${height - padding.bottom} Z`
          : "";

      const secondaryPath =
        showSecondary && secondaryValues.length > 0
          ? points
              .map((p, i) =>
                p.secondaryY !== undefined
                  ? `${i === 0 ? "M" : "L"} ${p.x}% ${p.secondaryY}`
                  : ""
              )
              .join(" ")
          : "";

      return { points, areaPath, linePath, yMin, yMax, secondaryPath };
    }, [data, height, showSecondary]);

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-[200px] text-[var(--text-muted)]">
        No data available
      </div>
    );
  }

  return (
    <div className="w-full">
      {title && (
        <h3 className="text-lg font-semibold text-[var(--text)] mb-4">
          {title}
        </h3>
      )}

      <div className="relative" style={{ height }}>
        <svg width="100%" height={height} className="overflow-visible">
          {/* Grid lines */}
          {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => (
            <line
              key={i}
              x1="50"
              y1={20 + ratio * (height - 60)}
              x2="100%"
              y2={20 + ratio * (height - 60)}
              stroke="var(--border)"
              strokeDasharray="4"
              strokeWidth="1"
            />
          ))}

          {/* Y-axis labels */}
          {[0, 0.5, 1].map((ratio, i) => (
            <text
              key={i}
              x="45"
              y={20 + ratio * (height - 60)}
              fill="var(--text-muted)"
              fontSize="12"
              textAnchor="end"
              dominantBaseline="middle"
            >
              {formatY(yMax - ratio * (yMax - yMin))}
            </text>
          ))}

          {/* Area fill */}
          {showArea && (
            <path
              d={areaPath}
              fill={`url(#areaGradient-${color.replace(/[^a-z0-9]/gi, "")})`}
              opacity="0.3"
            />
          )}

          {/* Main line */}
          <path
            d={linePath}
            fill="none"
            stroke={color}
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Secondary line (for ratings) */}
          {showSecondary && secondaryPath && (
            <path
              d={secondaryPath}
              fill="none"
              stroke={secondaryColor}
              strokeWidth="2"
              strokeDasharray="6,3"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          )}

          {/* Dots */}
          {showDots &&
            points.map((point, i) => (
              <g key={i}>
                <circle
                  cx={`${point.x}%`}
                  cy={point.y}
                  r="6"
                  fill="var(--surface)"
                  stroke={color}
                  strokeWidth="2"
                  className="cursor-pointer hover:r-8 transition-all"
                >
                  <title>
                    {point.label}: {formatY(point.value)}
                    {point.secondaryValue !== undefined &&
                      ` (Rating: ${point.secondaryValue.toFixed(1)})`}
                  </title>
                </circle>
                {showSecondary && point.secondaryY !== undefined && (
                  <circle
                    cx={`${point.x}%`}
                    cy={point.secondaryY}
                    r="4"
                    fill={secondaryColor}
                    className="cursor-pointer"
                  >
                    <title>Rating: {point.secondaryValue?.toFixed(1)}</title>
                  </circle>
                )}
              </g>
            ))}

          {/* X-axis labels (show every few) */}
          {points
            .filter(
              (_, i) =>
                i % Math.ceil(points.length / 8) === 0 ||
                i === points.length - 1
            )
            .map((point, i) => (
              <text
                key={i}
                x={`${point.x}%`}
                y={height - 10}
                fill="var(--text-muted)"
                fontSize="11"
                textAnchor="middle"
              >
                {point.label}
              </text>
            ))}

          {/* Gradient definition */}
          <defs>
            <linearGradient
              id={`areaGradient-${color.replace(/[^a-z0-9]/gi, "")}`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop offset="0%" stopColor={color} stopOpacity="0.4" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
          </defs>
        </svg>
      </div>

      {/* Legend */}
      {showSecondary && (
        <div className="flex items-center justify-center gap-6 mt-4">
          <div className="flex items-center gap-2">
            <div
              className="w-4 h-1 rounded"
              style={{ backgroundColor: color }}
            />
            <span className="text-xs text-[var(--text-muted)]">Films</span>
          </div>
          <div className="flex items-center gap-2">
            <Star className="w-3 h-3" style={{ color: secondaryColor }} />
            <span className="text-xs text-[var(--text-muted)]">Avg Rating</span>
          </div>
        </div>
      )}
    </div>
  );
}

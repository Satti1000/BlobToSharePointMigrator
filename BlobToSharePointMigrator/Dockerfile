FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["src/BlobToSharePointMigrator/BlobToSharePointMigrator.csproj", "BlobToSharePointMigrator/"]
RUN dotnet restore "BlobToSharePointMigrator/BlobToSharePointMigrator.csproj"
COPY src/BlobToSharePointMigrator/. BlobToSharePointMigrator/
WORKDIR "/src/BlobToSharePointMigrator"
RUN dotnet build "BlobToSharePointMigrator.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "BlobToSharePointMigrator.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "BlobToSharePointMigrator.dll"]

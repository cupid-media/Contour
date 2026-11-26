FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY . .

ARG VERSION=0.0.0
ARG CIUSER=user
ARG CITOKEN=CITOKEN

ENV NUGET_CERT_REVOCATION_MODE=offline
RUN dotnet build Sources/Contour/Contour.csproj -c Release

RUN dotnet nuget remove source pacman || true
RUN dotnet nuget add source "https://pacman-build.dev.dating/api/v4/projects/573/packages/nuget/index.json" --name nugetrep --username $CIUSER --password $CITOKEN --store-password-in-clear-text

RUN ls package/*.nupkg -lha &&\
    dotnet nuget push "package/Contour.$VERSION.nupkg" --source nugetrep

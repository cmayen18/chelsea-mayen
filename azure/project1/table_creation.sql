/******Object:  Table [dbo].[Track Data]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackDataSQL](
    [id] [nvarchar](200) NULL,
    [name] [nvarchar](100) NULL,
    [album] [nvarchar](100) NULL,
    [release_date] [nvarchar](100) NULL,
    [artist_id] INT NULL
    )
GO


/****** Object:  Table [dbo].[Track Features]   ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackFeaturesSQL](
    [id] [nvarchar](200) NULL,
    [length] [float] NULL,
    [popularity] [float] NULL,
    [danceability] [float] NULL,
    [acousticness] [float] NULL,
    [energy] [float] NULL,
    [liveness] [float] NULL,
    [speechiness] [float] NULL,
    [tempo] [float] NULL
    )
GO

/****** Object:  Table [dbo].[Artist Data]   ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ArtistDataSQL](
    [artist_id] [int] NULL,
    [artist] [nvarchar](200) NULL
    )
GO
